# -*- coding: utf-8 -*-
"""Collation support

"""
import cPickle
import sys

import dill

from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator, EvalMessage, EvaluatedMessage
from s3filter.plan.op_metrics import OpMetrics


class Collate(Operator):
    """This operator simply collects emitted tuples into a list. Useful for interrogating results at the end of a
    query to see if they are what is expected.

    """

    def __init__(self, name, query_plan, log_enabled):
        """Constructs a new Collate operator.

        """

        super(Collate, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.__tuples = []

    def tuples(self):
        """Accessor for the collated tuples

        :return: The collated tuples
        """

        if self.async_:
            self.queue.put(cPickle.dumps(EvalMessage("self.local_tuples()")))
            tuples = self.query_plan.listen(EvaluatedMessage).val
            return tuples
        else:
            return self.__tuples

    def local_tuples(self):
        """Accessor for the collated tuples

        :return: The collated tuples
        """

        return self.__tuples

    def on_receive(self, ms, _producer):
        """Handles the event of receiving a message from a producer. Will simply append the tuple to the internal
        list.

        :param ms: The received messages
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Collate | {}".format(t))
        for m in ms:
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

    def print_tuples(self, tuples=None):
        """Prints the tuples in tab separated format

        TODO: Needs some work to align properly

        :return:
        """

        print('')

        if tuples is None:
            tuples = self.__tuples

        field_names = False
        for t in tuples:
            if not field_names:

                # Write field name tuple

                field_names_len = 0
                t_iter = iter(t)
                first_field_name = True
                for f in t_iter:
                    if not first_field_name:
                        sys.stdout.write(' | ')
                    else:
                        first_field_name = False

                    sys.stdout.write(str(f))
                    field_names_len += len(str(f))

                sys.stdout.write('\n')
                print('-' * (field_names_len + ((len(t) - 1) * len(' | '))))
                field_names = True
            else:
                t_iter = iter(t)
                first_field_val = True
                for f in t_iter:
                    if not first_field_val:
                        sys.stdout.write(' | ')
                    else:
                        first_field_val = False

                    sys.stdout.write(str(f))

                print('')
