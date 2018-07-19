# -*- coding: utf-8 -*-
"""Collation support

"""

import sys
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage


class Collate(Operator):
    """This operator simply collects emitted tuples into a list. Useful for interrogating results at the end of a
    query to see if they are what is expected.

    """

    def __init__(self, name, log_enabled):
        """Constructs a new Collate operator.

        """

        super(Collate, self).__init__(name, OpMetrics(), log_enabled)

        self.__tuples = []

    def tuples(self):
        """Accessor for the collated tuples

        :return: The collated tuples
        """

        return self.__tuples

    def on_receive(self, m, _producer):
        """Handles the event of receiving a message from a producer. Will simply append the tuple to the internal
        list.

        :param m: The received message
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Collate | {}".format(t))
        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_):
        """Event handler for a received tuple

        :param tuple_: The received tuple
        :return: None
        """

        self.__tuples.append(tuple_)

    def write_to_file(self, out_file_name):
        """
        write tuples to out_file in tab separated format
        :param out_file_name: the file name to write tuples to
        :return:
        """
        with open(out_file_name, 'w') as out_file:
            self.write_to(out_file)

    def print_tuples(self):
        """Prints the tuples in tab separated format

        TODO: Needs some work to align properly

        :return:
        """

        print('')

        self.write_to(sys.stdout)

    def write_to(self, out_stream):
        field_names = False
        for t in self.__tuples:
            if not field_names:

                # Write field name tuple

                field_names_len = 0
                t_iter = iter(t)
                first_field_name = True
                for f in t_iter:
                    if not first_field_name:
                        out_stream.write(' | ')
                    else:
                        first_field_name = False

                    out_stream.write(str(f))
                    field_names_len += len(str(f))

                out_stream.write('\n')
                out_stream.write('-' * (field_names_len + ((len(t) - 1) * len(' | '))))
                out_stream.write('\n')
                field_names = True
            else:
                t_iter = iter(t)
                first_field_val = True
                for f in t_iter:
                    if not first_field_val:
                        out_stream.write(' | ')
                    else:
                        first_field_val = False

                    out_stream.write(str(f))
                out_stream.write('\n')
