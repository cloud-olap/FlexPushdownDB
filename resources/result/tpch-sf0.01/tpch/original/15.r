# When using arrow's group-by impl, it's computation has small round-off errors on different trials. This query joins
# two double-type group-by results, so using arrow's group-by impl may incur incorrect result occasinally.
# To be concrete: arrow's group-by result on "total_revenue" can be 1244231.8706999999 or 1244231.8707, which is
# reflected on the subsequent joins.

------------------------------------------------------------------------------------------------------------------------
| s_suppkey             | s_name                | s_address             | s_phone               | total_revenue
| int64                 | string                | string                | string                | double
------------------------------------------------------------------------------------------------------------------------
| 56                    | Supplier#000000056    | fUVtlUVal GiHBOuYoUQ XQ9NfNLQR3Gl| 26-471-195-5486       | 1244231.8706999999
------------------------------------------------------------------------------------------------------------------------
5 cols x 1 rows