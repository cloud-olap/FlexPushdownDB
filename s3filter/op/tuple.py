# -*- coding: utf-8 -*-
"""Tuple support

"""
from collections import OrderedDict


# class Tuple(list):
#     """Wrapper class for a tuple (which is in fact a list - at present anyway). Just a place for utility methods.
#
#     """
#
#     def __init__(self, seq=()):
#         """Creates a new tuple.
#
#         :param seq: The sequence to create the tuple from
#         """
#         super(Tuple, self).__init__(seq)

# Tuple is just a type alias for list
Tuple = list

# class LabelledTuple(Tuple):
#     """Wrapper class for a tuple which overlays labels. Simply means a tuple field can be accessed via a
#     label (aka a column name/alias).
#
#     """
#
#     def __init__(self, seq=(), labels=None):
#         """Creates a new labelled tuple. Each tuple element will be accessible via the labels supplied.
#
#         :param seq: The tuple sequence
#         :param labels: Labels to apply to each tuple element. If none are supplied the labels will default to
#             '_0', '_1' and so on.
#         """
#
#         super(LabelledTuple, self).__init__(seq)
#
#         if labels is None:
#             # If no labels supplied, create defaults
#             labels = []
#             for i in range(0, len(seq)):
#                 label = '_' + str(i)
#                 labels.append(label)
#
#         self.labels = labels
#
#     def __getitem__(self, item):
#         """Tuple element accessor.
#
#         :param item: The label to access the tuple via.
#         :return: The accessed tuple field.
#         """
#
#         if type(item) is str:
#             if item not in self.labels:
#                 raise Exception("Label {} is not in tuple labels {}".format(item, self.labels))
#             else:
#                 i = self.labels.index(item)
#                 return super(LabelledTuple, self).__getitem__(i)
#         else:
#             return super(LabelledTuple, self).__getitem__(item)
#
#     def __contains__(self, item):
#         return self.labels.__contains__(item)


class IndexedTuple(Tuple):
    """This class allows tuple fields to be accessed by field name. It keeps a field names index which is a dict of
    field names to the index where a field value appears in a tuple.

    Since we will be working with lots of tuples where the field name index is often the same (e.g. a long list of
    row tuples with the same structure) we don't want to store a separate index along with each tuple, which is how
    a Python dict or named tuple would work. By accepting a field names index in the constructor all we need to maintain
    is a single pointer in each IndexedTuple.

    Functions are provided for generating default names, converting a tuple and a tuple of field names into an
    IndexedTuple, and for extracting the field names index in order as a regular tuple.

    """

    def __init__(self, t, field_names_index):
        """

        :param t: The tuple
        :param field_names_index: The dict of tuple field names to indexes in the tuple
        """

        super(IndexedTuple, self).__init__(t)
        self.__field_names_index = field_names_index

    @staticmethod
    def build_default(t):

        field_names_index = OrderedDict()
        for i in range(0, len(t)):
            field_name = '_' + str(i)
            field_names_index[field_name] = i

        return IndexedTuple(t, field_names_index)

    @staticmethod
    def build(t, field_names):
        return IndexedTuple(t, IndexedTuple.build_field_names_index(field_names))

    @staticmethod
    def build_field_names_index(field_names):

        field_names_index = OrderedDict()
        for i, fn in enumerate(field_names):
            field_names_index[fn] = i

        return field_names_index

    def field_names(self):
        return self.__field_names_index.keys()

    def __getitem__(self, item):
        """Tuple element accessor.

        :param item: The label to access the tuple via.
        :return: The accessed tuple field.
        """

        if type(item) is str:
            try:
                return super(IndexedTuple, self).__getitem__(self.__field_names_index[item])
            except KeyError:
                raise Exception("Field '{}' is not in field names index {}".format(item, self.__field_names_index))
        else:
            return super(IndexedTuple, self).__getitem__(item)

    def __contains__(self, item):
        if type(item) is str:
            return self.__field_names_index.__contains__(item)
        else:
            return self.__contains__(item)
