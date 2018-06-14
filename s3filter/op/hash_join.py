# -*- coding: utf-8 -*-
"""Join support

"""
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.tuple import Tuple, IndexedTuple


class HashJoinMetrics(OpMetrics):
    """Extra metrics for a project

    """

    def __init__(self):
        super(HashJoinMetrics, self).__init__()

        self.l_rows_processed = 0
        self.r_rows_processed = 0
        self.rows_joined = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'l_rows_processed': self.l_rows_processed,
            'r_rows_processed': self.r_rows_processed,
            'rows_joined': self.rows_joined
        }.__repr__()


class HashJoin(Operator):
    """Implements a inner join using hash tables.

    """

    def __init__(self, join_expr, name, log_enabled):
        """
        Creates a new join operator.

        :param join_expr: The join expression indicating which fields of which key to join on
        """

        super(HashJoin, self).__init__(name, HashJoinMetrics(), log_enabled)

        self.join_expr = join_expr

        self.__l_tuples = []
        self.__r_tuples = []

        self.__l_field_names = None
        self.__r_field_names = None

        self.__l_producer_name = None
        self.__r_producer_name = None

        self.__l_producer_completed = False
        self.__r_producer_completed = False

    def connect_left_producer(self, producer):
        """Connects a producer as the producer of left tuples in the join expression

        :param producer: The left producer
        :return: None
        """

        if self.__l_producer_name is not None:
            raise Exception("Only 1 left producer can be added. Left producer '{}' already added"
                            .format(self.__l_producer_name))

        if producer.name is self.__r_producer_name:
            raise Exception("Producer cannot be added as both left and right producer. "
                            "Producer '{}' already added as right producer"
                            .format(self.__l_producer_name))

        self.__l_producer_name = producer.name

        Operator.connect(producer, self)

    def connect_right_producer(self, producer):
        """Connects a producer as the producer of right tuples in the join expression

        :param producer: The right producer
        :return: None
        """

        if self.__r_producer_name is not None:
            raise Exception("Only 1 right Producer can be added. Right producer '{}' already added"
                            .format(self.__r_producer_name))

        if producer.name is self.__l_producer_name:
            raise Exception("Producer cannot be added as both right and left producer. "
                            "Producer '{}' already added as left producer"
                            .format(self.__l_producer_name))

        self.__r_producer_name = producer.name
        Operator.connect(producer, self)

    def on_receive(self, m, producer):
        """Handles the event of receiving a new message from a producer.

        :param m: The received message
        :param producer: The producer of the tuple
        :return: None
        """

        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_, producer)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_, producer):
        """Check that the tuple has been produced by a connected producer, and contains a field in the
        join expression. If it is from a producer we haven't seen before store its data as field names, otherwise add
        it as a values tuple.

        :param tuple_: The received tuple
        :param producer: The producer of the tuple
        :return: None
        """

        # Check the producer is connected
        if self.__l_producer_name is None:
            raise Exception("Left producer is not connected")

        if self.__r_producer_name is None:
            raise Exception("Right producer is not connected")

        # Check which producer sent the tuple
        if producer.name == self.__l_producer_name:

            if self.__l_field_names is None:
                if self.join_expr.l_field in tuple_:
                    self.__l_field_names = tuple_
                else:
                    raise Exception("Join Operator '{}' received invalid left field names tuple {}. "
                                    "Tuple must contain join left field name '{}'."
                                    .format(self.name, tuple_, self.join_expr.l_field))
            else:

                self.op_metrics.l_rows_processed += 1

                self.__l_tuples.append(tuple_)

        elif producer.name == self.__r_producer_name:

            if self.__r_field_names is None:
                if self.join_expr.r_field in tuple_:
                    self.__r_field_names = tuple_
                else:
                    raise Exception("Join Operator '{}' received invalid right field names tuple {}. "
                                    "Tuple must contain join right field name '{}'."
                                    .format(self.name, tuple_, self.join_expr.r_field))
            else:

                self.op_metrics.r_rows_processed += 1

                self.__r_tuples.append(tuple_)

        else:
            raise Exception(
                "Join Operator '{}' received invalid tuple {} from producer '{}'. "
                "Tuple must be sent from connected left producer '{}' or right producer '{}'."
                .format(self.name, tuple_, producer.name, self.__l_producer_name, self.__r_producer_name))

    def on_producer_completed(self, producer):
        """Handles the event where a producer has completed producing all the tuples it will produce. Note that the
        Join operator may have multiple producers. Once all producers are complete the operator can send the tuples
        it contains to downstream consumers.

        :type producer: The producer that has completed
        :return: None
        """

        if producer.name is self.__l_producer_name:
            self.__l_producer_completed = True

        if producer.name is self.__r_producer_name:
            self.__r_producer_completed = True

        # Check that we have received a completed event from all the producers
        is_all_producers_done = self.__l_producer_completed & self.__r_producer_completed

        if self.log_enabled:
            print("{}('{}') | Producer completed [{}]".format(
                self.__class__.__name__,
                self.name,
                {'completed_producer': producer.name, 'all_producers_completed': is_all_producers_done}))

        if is_all_producers_done and not self.is_completed():

            # Join and send the field names first
            self.join_field_names()

            # Join and send the joined data tuples
            self.join_field_values()

            Operator.on_producer_completed(self, producer)

    def join_field_values(self):
        """Performs the join on data tuples using a nested loop joining algorithm. The joined tuples are each sent.
        Allows for the loop to be broken if the operator completes while executing.

        :return: None
        """

        # Determine which direction the hash join should run
        # The larger relation should remain as a list and the smaller relation should be hashed. The larger relation
        # should be iterated as a list
        if len(self.__l_tuples) > len(self.__r_tuples):
            l_to_r = True
            r_to_l = not l_to_r
        else:
            l_to_r = False
            r_to_l = not l_to_r

        if l_to_r:
            outer_tuples_list = self.__l_tuples
            inner_tuples_list = self.__r_tuples
            inner_tuple_field_name = self.join_expr.r_field
            inner_tuple_field_names = self.__r_field_names
            outer_tuple_field_index = self.__l_field_names.index(self.join_expr.l_field)
        else:
            outer_tuples_list = self.__r_tuples
            inner_tuples_list = self.__l_tuples
            inner_tuple_field_name = self.join_expr.l_field
            inner_tuple_field_names = self.__l_field_names
            outer_tuple_field_index = self.__r_field_names.index(self.join_expr.r_field)

        # Hash the tuples from the smaller set of tuples
        inner_tuples_dict = {}
        for t in inner_tuples_list:
            lt = IndexedTuple.build(t, inner_tuple_field_names)
            inner_tuples_dict[lt[inner_tuple_field_name]] = t

        for outer_tuple in outer_tuples_list:

            if self.is_completed():
                break

            outer_tuple_field_value = outer_tuple[outer_tuple_field_index]
            inner_tuple = inner_tuples_dict.get(outer_tuple_field_value, None)

            if inner_tuple is not None:

                if l_to_r:
                    t = outer_tuple + inner_tuple
                else:
                    t = inner_tuple + outer_tuple

                if self.log_enabled:
                    print("{}('{}') | Sending field values [{}]".format(
                        self.__class__.__name__,
                        self.name,
                        {'data': t}))

                self.op_metrics.rows_joined += 1

                self.send(TupleMessage(Tuple(t)), self.consumers)

    def join_field_names(self):
        """Examines the collected field names and joins them into a single list, left field names followed by right
        field names. The joined field names tuple is then sent.

        :return: None
        """

        joined_field_names = []

        # We can only emit field name tuples if we received tuples for both sides of the join
        if self.__l_field_names is not None and self.__r_field_names is not None:

            for field_name in self.__l_field_names:
                joined_field_names.append(field_name)

            for field_name in self.__r_field_names:
                joined_field_names.append(field_name)

            if self.log_enabled:
                print("{}('{}') | Sending field names [{}]".format(
                    self.__class__.__name__,
                    self.name,
                    {'field_names': joined_field_names}))

            self.send(TupleMessage(Tuple(joined_field_names)), self.consumers)
