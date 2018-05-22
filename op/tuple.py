# -*- coding: utf-8 -*-
"""Tuple support

"""


class Tuple(list):
    """Wrapper class for a tuple (which is in fact a list - at present anyway). Just a place for utility methods.

    """

    def __init__(self, seq=()):
        """Creates a new tuple.

        :param seq: The sequence to create the tuple from
        """
        super(Tuple, self).__init__(seq)


class LabelledTuple(Tuple):
    """Wrapper class for a tuple which overlays labels. Simply means a tuple field can be accessed via a
    label (aka a column name/alias).

    """

    def __init__(self, seq=(), labels=None):
        """Creates a new labelled tuple. Each tuple element will be accessible via the labels supplied.

        :param seq: The tuple sequence
        :param labels: Labels to apply to each tuple element. If none are supplied the labels will default to
            '_0', '_1' and so on.
        """

        super(LabelledTuple, self).__init__(seq)

        if labels is None:
            # If no labels supplied, create defaults
            labels = []
            for i in range(0, len(seq)):
                label = '_' + str(i)
                labels.append(label)

        self.labels = labels

    def __getitem__(self, item):
        """Tuple element accessor.

        :param item: The label to access the tuple via.
        :return: The accessed tuple field.
        """
        i = self.labels.index(item)
        return super(LabelledTuple, self).__getitem__(i)
