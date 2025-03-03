"""Test common.metrics."""

from unittest.mock import MagicMock, patch

import pytest

from alpenhorn.common import metrics

prometheus_client = pytest.importorskip(
    "prometheus_client", exc_type=ModuleNotFoundError
)
Counter = prometheus_client.metrics.Counter
Gauge = prometheus_client.metrics.Gauge
REGISTRY = prometheus_client.registry.REGISTRY


@pytest.fixture
def cleanup():
    """Clean up after metric tests."""
    yield

    # Delete all metrics from the prometheus client registry and our dict
    while metrics._metrics:
        key, value = metrics._metrics.popitem()
        REGISTRY.unregister(value[0])


def test_nothing(cleanup):
    """Test nothing.

    The only point of this test is to trigger the cleanup fixture so
    everything following starts with an empty _metric cache.
    """
    assert True


def test_label_clash(cleanup):
    """Can't both bind a label and leave it unbound."""
    with pytest.raises(KeyError):
        metrics.Metric("name", "desc", unbound={"a", "b", "c"}, bound={"c": "d"})


def test_label_change(cleanup):
    """Accessing the same metric with different labels shouldn't work."""

    metrics.Metric("name", "desc", unbound={"a", "b", "c"})

    with pytest.raises(ValueError):
        # Missing a label
        metrics.Metric("name", "desc", unbound={"a", "b"})

    with pytest.raises(ValueError):
        # Extra label
        metrics.Metric("name", "desc", unbound={"a", "b", "c", "d"})

    with pytest.raises(ValueError):
        # Missing and extra label
        metrics.Metric("name", "desc", unbound={"a", "b", "d"})


def test_labelnames(cleanup):
    """Test Metric.labelnames"""
    metric = metrics.Metric("unbound", "desc", unbound={"h", "b", "c", "d", "f"})
    assert metric.labelnames == ["b", "c", "d", "f", "h"]

    metric = metrics.Metric(
        "mixed", "desc", unbound={"a", "b", "c"}, bound={"d": "e", "f": "g"}
    )
    assert metric.labelnames == ["a", "b", "c", "d", "f"]

    metric = metrics.Metric(
        "bound", "desc", bound={"i": "a", "b": "b", "c": "c", "d": "e", "f": "g"}
    )
    assert metric.labelnames == ["b", "c", "d", "f", "i"]


def test_labelvalues(cleanup):
    """Test Metric.labelvalues"""
    metric = metrics.Metric("unbound", "desc", unbound={"h", "b", "c", "d", "f"})
    assert metric.labelvalues(h=1, b=2, c=3, d=4, f=5) == [2, 3, 4, 5, 1]

    metric = metrics.Metric(
        "mixed", "desc", unbound={"a", "b", "c"}, bound={"d": "e", "f": "g"}
    )
    assert metric.labelvalues(a=6, b=7, c=8) == [6, 7, 8, "e", "g"]

    metric = metrics.Metric(
        "bound", "desc", bound={"i": "a", "b": "b", "c": "c", "d": "e", "f": "g"}
    )
    assert metric.labelvalues() == ["b", "c", "e", "g", "a"]


def test_labelvalues_bad(cleanup):
    """Test invalid labels to labelvalues()"""
    metric = metrics.Metric(
        "name", "desc", unbound={"a", "b", "c"}, bound={"d": "e", "f": "g"}
    )

    # must bind all labels
    with pytest.raises(ValueError):
        metric.labelvalues(a=1)

    # Can't add extra labels
    with pytest.raises(TypeError):
        metric.labelvalues(a=1, b=2, c=3, h=4)

    # Can't re-bind bound labels
    with pytest.raises(TypeError):
        metric.labelvalues(a=1, b=2, c=3, d=4)


def test_bind(cleanup):
    """Test Metric.bind"""
    base = metrics.Metric("name", "desc", unbound=["a", "b", "c"])

    # All labels are unbound
    assert base.labelvalues(a=1, b=2, c=3) == [1, 2, 3]

    # Can't bind non-existent labels
    with pytest.raises(TypeError):
        base.bind(d=0)

    # This is fine.  It just makes a copy.
    copy = base.bind()

    # All labels are still unbound
    assert copy.labelvalues(a=1, b=2, c=3) == [1, 2, 3]

    # Not the same instance as base
    assert copy is not base

    # Bind some labels
    copy = base.bind(a=4)

    # Only two unbound labels, now
    assert copy.labelvalues(b=2, c=3) == [4, 2, 3]

    # Fails because a is now bound
    with pytest.raises(TypeError):
        assert copy.labelvalues(a=1, b=2, c=3) == [1, 2, 3]

    # But the base hasn't changed
    assert base.labelvalues(a=1, b=2, c=3) == [1, 2, 3]

    # Bind the remaining labels
    copy2 = copy.bind(b=5, c=6)

    # Now no unbound labels
    assert copy2.labelvalues() == [4, 5, 6]


def test_labelled_metric(cleanup):
    """Test that _labelled_metric does return a prometheus metric."""
    gauge = metrics.Metric(
        "gauge", "desc", unbound={"a", "b", "c"}, bound={"d": "e", "f": "g"}
    )

    submetric = gauge._labelled_metric({"a": "a", "b": "b", "c": "c"})
    assert isinstance(submetric, Gauge)

    counter = metrics.Metric(
        "counter",
        "desc",
        counter=True,
        unbound={"a", "b", "c"},
        bound={"d": "e", "f": "g"},
    )
    submetric = counter._labelled_metric({"a": "a", "b": "b", "c": "c"})
    assert isinstance(submetric, Counter)


def test_unlabelled_metric(cleanup):
    """Test using an unlabelled metric."""

    # In this case, Metric._labelled_metric _can't_ call the
    # prom client metric's .label().  So, it just returns the parent
    # metric, which is then used for the .set call

    metric = metrics.Metric("name", "desc")

    # _labelled_metric just returns _metric now
    assert metric._labelled_metric(labels={}) is metric._metric


def test_add(cleanup):
    """Test Metric.add"""

    gauge = metrics.Metric(
        "gauge", "desc", unbound=["a", "b", "c"], bound={"d": "e", "f": "g"}
    )

    # Replace the internal prom metric with some mocks (childmock plays
    # the role of the labelled child metric)
    mock = MagicMock()
    childmock = MagicMock()
    mock.labels.return_value = childmock
    gauge._metric = mock

    gauge.add(1, a=1, b=2, c=3)

    mock.labels.assert_called_with(a=1, b=2, c=3, d="e", f="g")
    # .add is implemented via .inc on the prom metric
    childmock.inc.assert_called_with(1)

    # Not fully bound is an error
    with pytest.raises(ValueError):
        gauge.add(0, b=5, c=6)

    gauge.add(-3, a=4, b=5, c=6)

    mock.labels.assert_called_with(a=4, b=5, c=6, d="e", f="g")
    childmock.inc.assert_called_with(-3)


def test_inc(cleanup):
    """Test Metric.inc"""

    metric = metrics.Metric(
        "name", "desc", unbound=["a", "b", "c"], bound={"d": "e", "f": "g"}
    )

    # Replace the internal prom metric with some mocks (childmock plays
    # the role of the labelled child metric)
    mock = MagicMock()
    childmock = MagicMock()
    mock.labels.return_value = childmock
    metric._metric = mock

    metric.inc(a=1, b=2, c=3)

    mock.labels.assert_called_with(a=1, b=2, c=3, d="e", f="g")
    # The passed .inc value is always explicit
    childmock.inc.assert_called_with(1)

    # Not fully bound is an error
    with pytest.raises(ValueError):
        metric.inc(b=5, c=6)


def test_dec(cleanup):
    """Test Metric.dec"""

    metric = metrics.Metric(
        "name", "desc", unbound=["a", "b", "c"], bound={"d": "e", "f": "g"}
    )

    # Replace the internal prom metric with some mocks (childmock plays
    # the role of the labelled child metric)
    mock = MagicMock()
    childmock = MagicMock()
    mock.labels.return_value = childmock
    metric._metric = mock

    metric.dec(a=1, b=2, c=3)

    mock.labels.assert_called_with(a=1, b=2, c=3, d="e", f="g")
    childmock.inc.assert_called_with(-1)

    # Not fully bound is an error
    with pytest.raises(ValueError):
        metric.dec(b=5, c=6)


def test_set_gauge(cleanup):
    """Test Metric.set on a gauge"""

    gauge = metrics.Metric(
        "name", "desc", unbound=["a", "b", "c"], bound={"d": "e", "f": "g"}
    )

    # Replace the internal prom metric with some mocks (childmock plays
    # the role of the labelled child metric)
    mock = MagicMock()
    childmock = MagicMock()
    mock.labels.return_value = childmock
    gauge._metric = mock

    # Not fully bound is an error
    with pytest.raises(ValueError):
        gauge.set(-1, b=5, c=6)

    gauge.set(-2, a=4, b=5, c=6)

    # We always use the keyword calling convention with .labels
    mock.labels.assert_called_with(a=4, b=5, c=6, d="e", f="g")
    childmock.set.assert_called_with(-2)


def test_set_counter(cleanup):
    """Test Metric.set on a counter"""

    counter = metrics.Metric(
        "name",
        "desc",
        counter=True,
        unbound=["a", "b", "c"],
        bound={"d": "e", "f": "g"},
    )

    # Replace the internal prom metric with some mocks (childmock plays
    # the role of the labelled child metric)
    mock = MagicMock()
    childmock = MagicMock()
    mock.labels.return_value = childmock
    counter._metric = mock

    # Not fully bound is an error
    with pytest.raises(ValueError):
        counter.set(0, b=5, c=6)

    # Can't use a non-zero value
    with pytest.raises(ValueError):
        counter.set(2, a=4, b=5, c=6)

    # This resets the counter
    counter.set(0, a=4, b=5, c=6)

    mock.labels.assert_called_with(a=4, b=5, c=6, d="e", f="g")
    childmock.reset.assert_called()
    childmock.set.assert_not_called()


def test_remove(cleanup):
    """Test Metric.remove"""

    metric = metrics.Metric(
        "name", "desc", unbound=["a", "b", "c"], bound={"d": "e", "f": "g"}
    )

    # Replace the internal prom metric with a mock
    mock = MagicMock()
    metric._metric = mock

    # Not fully bound is an error
    with pytest.raises(ValueError):
        metric.remove(b=5, c=6)

    metric.remove(a=4, b=5, c=6)

    # No keywords allowed here; must pass the ordered labelvalue list
    mock.remove.assert_called_with([4, 5, 6, "e", "g"])


def test_by_name(cleanup):
    """Test by_name."""

    metric = metrics.by_name("transfers")
    assert isinstance(metric, metrics.Metric)

    with pytest.raises(ValueError):
        metrics.by_name("no_such_metric")


@pytest.mark.alpenhorn_config({"daemon": {"prom_client_port": "0"}})
def test_start_promclient_off(set_config):
    """Test start_promclient with no port."""

    mock = MagicMock()
    with patch("prometheus_client.start_http_server", mock):
        metrics.start_promclient()

    mock.assert_not_called()


@pytest.mark.alpenhorn_config({"daemon": {"prom_client_port": "1234"}})
def test_start_promclient_on(set_config):
    """Test start_promclient with a port."""

    mock = MagicMock()
    with patch("prometheus_client.start_http_server", mock):
        metrics.start_promclient()

    mock.assert_called_with(1234)
