"""Alpenhorn interface to prometheus_client

This module provides an interface between alpenhorn
and the prometheus client.

Metrics should be created and accessed through the `Metric` class, which
is a light-weight wrapper around the prometheus_client's Counter and
Gauge metrics.

The `Metric` class adds a restriction that all instances of a metric of a
given name have the same set of labels.  The `Metric` class also provides a
mechanism to bind particular labels, meaning repetition of common label
values is not necessary.

If the `prometheus_client` module cannot be imported, most of this module
does nothing.
"""

from __future__ import annotations

from ..common import config

try:
    import prometheus_client as prom
except ModuleNotFoundError:
    prom = None

# A dict of all the metrics we're using.  Keys are names.
# Values are two-tuples with elements:
#  * the prometheus_client metric object
#  * the set of all labelnames
_metrics = {}


class Metric:
    """A wrapper for prometheus_client metrics."""

    def __init__(
        self,
        name: str,
        description: str,
        counter: bool = False,
        unbound: list | tuple | set = (),
        bound: dict = {},
    ) -> None:
        """Create a new metric.

        Labels in the metric can be bound (set to a fixed value) or unbound
        (with no fixed value).  Names of unbound labels are listed in `unbound`.
        Bound labels with their values are set by the `bound` dict.

        All instances of a metric of a given `name` must have the same set of
        label names (but different instances may change which of those are bound
        and which are unbound), They must also have the same type (i.e. the same
        value for `counter`).  All instances with the same `name` control the same
        underlying metric.

        Newly-created metrics are implicitly set to zero, but, unless they have
        no labels at all, they won't actually be reported until their value is
        explicitly set/updated.

        Parameters
        ----------
        name:
            Name of the metric, excluding the initial `alpenhorn_`.
        description:
            A human-readable description of the data in the metric.
        counter:
            If True, create a Counter metric.  Otherwise a Gauge metric is created.
        unbound:
            A set (or list or tuple) of names of unbound labels.
        bound:
            A dict of label key-value pairs for bound labels.

        Raises
        ------
        KeyError:
            The same label name was specified in both `unbound` and `bound`.
        TypeError:
            An attempt was made to access an existing metric using the wrong
            value for `counter`.
        ValueError:
            An attempt was made to access an existing metric using the wrong
            set of label names.
        """
        global _metrics

        # keys in "bound" can't also appear in "unbound"
        for key in bound.keys():
            if key in unbound:
                raise KeyError(f'label "{key}" is both bound and unbound')

        self._name = name
        self._desc = description
        self._unbound_labels = set(unbound)
        self._bound_labels = bound
        self._counter = counter

        # We don't save any metrics, if we have no prometheus_client
        if prom is None:
            self._metric = None
            return

        # Metric type
        _type = prom.Counter if counter else prom.Gauge

        # Get or create the prom metric
        if name in _metrics:
            existing_metric, existing_labels = _metrics[name]
            # Validate access: must be using the correct type and list of labels
            if not isinstance(existing_metric, _type):
                raise TypeError(f"wrong metric type for metric: {name}")
            # Make sure labelnames is the same
            if existing_labels != set(self.labelnames):
                raise ValueError(
                    f"wrong labels for metric.  Expected: {existing_labels}"
                )
            self._metric = existing_metric
        else:
            self._metric = _type(
                "alpenhorn_" + name, description, labelnames=self.labelnames
            )
            _metrics[name] = (self._metric, set(self.labelnames))

    def bind(self, **labels: str) -> Metric:
        """Bind values to labels.

        This method returns a _copy_ of the Metric with the currently unbound
        labels newly bound to the values specified by the keywords.  Not all
        unbound labels need be specified.  Unbound labels not specified remain
        unbound in the returned copy.

        The original Metric is left unchanged.

        Raises
        ------
        TypeError:
            A keyword parameter did not correspond to one of the unbound labels.
        """
        unbound = set(self._unbound_labels)
        bound = self._bound_labels.copy()

        # Bind the new label values
        for key in labels.keys():
            if key not in unbound:
                raise TypeError(f'"{key}" is not an unbound label')
            unbound.remove(key)
            bound[key] = labels[key]

        # Return the child
        return Metric(
            name=self._name,
            description=self._desc,
            counter=self._counter,
            unbound=unbound,
            bound=bound,
        )

    def _check_unbound_covered(self, labels: dict) -> None:
        """Check cover of given labels.

        Checks that `labels` provides a value for all unbound labels
        (and nothing more).

        Raises
        ------
        TypeError:
            An extra label was included.
        ValueError:
            A label was missing.
        """

        keys = set(labels.keys())

        missing_keys = self._unbound_labels - keys
        if missing_keys:
            raise ValueError("not bound: " + ", ".join(missing_keys))

        extra_keys = keys - self._unbound_labels
        if extra_keys:
            raise TypeError("not unbound: " + ", ".join(extra_keys))

    @property
    def labelnames(self) -> list[str]:
        """An ordered list of label names."""

        return sorted(self._unbound_labels | set(self._bound_labels.keys()))

    def labelvalues(self, **labels: str) -> list[str]:
        """Return the list of label values.

        Values for all unbound labels must be specified by keyword.

        The returned list is guaranteed to be in the same order as the list
        of label names given by `labelnames`.
        """
        self._check_unbound_covered(labels)

        # Merge the labels
        labels |= self._bound_labels

        # Return values in the correct order
        return [labels[key] for key in sorted(labels.keys())]

    def _labelled_metric(self, labels: dict) -> prom.metrics.MetricWrapperBase:
        """Returns the labelled metric.

        Returns a `prometheus_client` child metric for the labelset resulting
        from merging the unbound and bound labels.

        Values for all unbound labels must be specified in the supplied `labels`
        dict.
        """
        # Unlabelled metrics have no children, so we must return the parent
        if not self._unbound_labels and not self._bound_labels:
            return self._metric

        self._check_unbound_covered(labels)

        if self._metric:
            return self._metric.labels(**labels, **self._bound_labels)
        return None

    def add(self, value: float, /, **labels: str) -> None:
        """Add `value` to the metric.

        For Counter metrics, `value` must be positive.

        Values for all unbound labels must be specified in the supplied `labels`
        dict.
        """
        if self._metric:
            self._labelled_metric(labels).inc(value)

    def inc(self, /, **labels: str) -> None:
        """Increment the metric by one.

        Values for all unbound labels must be specified in the supplied `labels`
        dict.
        """
        self.add(1, **labels)

    def dec(self, /, **labels: str) -> None:
        """Decrement the metric by one.

        Calling this metric on a counter will result in an error.

        Values for all unbound labels must be specified in the supplied `labels`
        dict.
        """
        # We'll let prometheus_client throw the exception for counters.
        self.add(-1, **labels)

    def set(self, value: float, /, **labels: str) -> None:
        """Set the metric to `value`.

        For counter metrics, the only allowed value is zero.

        Values for all unbound labels must be specified in the supplied `labels`
        dict.
        """
        if self._metric is None:
            return

        if self._counter:
            # For counters, Metric.set(0) is converted into a `.reset()` call
            if value:
                raise ValueError("attempt to set counter to non-zero value")
            self._labelled_metric(labels).reset()
        else:
            # Gauges have a .set() method.
            self._labelled_metric(labels).set(value)

    def remove(self, /, **labels: str) -> None:
        """Remove a labelset from the metric.

        Deletes the child metric with the labelset resulting from merging the
        unbound and bound labels.

        Values for all unbound labels must be specified in the supplied `labels`
        dict.

        If no such child metric exists, this function does nothing and succeeds.

        Paramters:
        ----------
        labels:
            The key-value pairs of the unbound labels for the labelset.
        """
        if self._metric:
            try:
                self._metric.remove(self.labelvalues(**labels))
            except KeyError:
                pass  # Child metric didn't exist

    def clear(self) -> None:
        """Remove all labelsets from the metric.

        The metric, per se, is not deleted.
        """
        if self._metric:
            self._metric.clear()


def by_name(name: str) -> Metric:
    """Retrieve the pre-made Metric called `name`.

    This function returns several pre-made Metric instances for use in cases
    where there is no other natural place to define the metric.

    Metrics returned by this function have no bound labels.  You may call
    `.bind()` on the returned metric if you wish to bind some of them.

    Parameters
    ----------
    name:
        The name of the Metric to return

    Raises
    ------
    ValueError:
        No such Metric was found with the requested name.
    """

    if name == "requests_completed":
        return Metric(
            name,
            "Count of completed requests",
            counter=True,
            unbound=("type", "result", "node", "group"),
        )
    if name == "transfers":
        return Metric(
            name,
            "Count of transfer attempts",
            counter=True,
            unbound=("result", "node_from", "group_to"),
        )

    raise ValueError(f"no such metric: {name}")


def start_promclient() -> None:
    """Start the prometheus client

    The client is only started if `daemon.prom_client_port`
    is set to a positive value in the alpenhorn config.
    """

    # Get the port number.  If not found, we just return here.
    try:
        port = int(config.config["daemon"]["prom_client_port"])
        if port <= 0:
            return
    except KeyError:
        return

    # Okay, we're good to start the http server, if we can
    if prom is not None:
        prom.disable_created_metrics()
        prom.start_http_server(port)
