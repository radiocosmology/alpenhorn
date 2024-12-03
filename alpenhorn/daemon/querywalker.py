"""The QueryWalker class"""

import peewee as pw
from peewee import fn


class QueryWalker:
    """QueryWalker class

    Given a peewee table `model` and collection of query `expression`s,
    this class will start in a random position in the middle and
    loop through the query, returning successive records.

    Parameters
    ----------
    model : peewee.Model
        The table model to walk
    *expressions : peewee.Expression
        Zero or more `peewee.Expression`s (i.e. `where()`-clause
        arguments) used to limit the rows walked over.

    Raises
    ------
    peewee.DoesNotExist
        The query produced no results
    """

    __slots__ = ["_expressions", "_id", "_model"]

    def __init__(self, model: pw.Model, *expressions: pw.Expression) -> None:
        self._model = model
        self._expressions = expressions

        # Figure out which randomising function to use.
        # (model._meta.database is actualy the proxy, so we check its obj)
        if isinstance(self._model._meta.database.obj, pw.MySQLDatabase):
            rand = fn.Rand
        else:
            rand = fn.Random

        # Start in a random location
        q = model.select(model.id).where(*expressions).order_by(rand()).limit(1)
        self._id = q.scalar()
        if self._id is None:
            raise pw.DoesNotExist("no records matched query")

    def get(self, n: int = 1) -> list[pw.Model]:
        """Retrieve `n` items from the current location

        Loops around to the beginning when it gets to the end.

        Parameters
        ----------
        n : int, optional
            The number of items to return

        Returns
        -------
        records : list of model instances
            The records returned.  If `n` is greater than the total
            number of matching records, duplicate records will be
            returned.


        Raises
        ------
        ValueError
            `n` < 1
        peewee.DoesNotExist
            the query produced no results

        Notes
        -----
        This always returns a list of `n` records, even when n == 1.
        """
        if n < 1:
            raise ValueError("n must be positive")

        # This may return nothing because we've gone past the
        # end of the query, so don't check for no items yet.
        items = list(
            self._model.select()
            .where(*self._expressions, self._model.id >= self._id)
            .order_by(self._model.id)
            .limit(n)
        )

        # Get more items by going back to the beginning if necessary
        n -= len(items)
        while n > 0:
            # Starting at the beginning just means not
            # having to include the >= self._id constraint
            more_items = list(
                self._model.select()
                .where(*self._expressions)
                .order_by(self._model.id)
                .limit(n)
            )

            # If this returned nothing, clearly the query
            # is no longer producing results
            if len(more_items) == 0:
                raise pw.DoesNotExist("no records matched query")
            items += more_items
            n -= len(more_items)

        # Save the current position for next time.
        self._id = 1 + items[-1].id

        return items
