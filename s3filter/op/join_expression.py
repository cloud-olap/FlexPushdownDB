class JoinExpression(object):
    """Represents a join expression, as in the column name (field) to join on.

    """

    def __init__(self, l_field, r_field):
        """Creates a new join expression.

        :param l_field: Field of the left tuple to join on
        :param r_field: Field of the right tuple to join on
        """

        self.l_field = l_field

        self.r_field = r_field

    def __repr__(self):
        return {
            'l_field': self.l_field,
            'r_field': self.r_field
        }.__repr__()
