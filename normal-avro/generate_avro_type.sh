
# Stable Schemas Generation
avrogencpp -i ./schemas/stable/customer.json -o ./include/customer.hh -n i
avrogencpp -i ./schemas/stable/date.json -o ./include/date.hh -n i
avrogencpp -i ./schemas/stable/lineorder.json -o ./include/lineorder.hh -n i
avrogencpp -i ./schemas/stable/part.json -o ./include/part.hh -n i
avrogencpp -i ./schemas/stable/supplier.json -o ./include/supplier.hh -n i

# Delta Schemas Generation
avrogencpp -i ./schemas/delta/customer_d.json -o ./include/customer_d.hh -n i
avrogencpp -i ./schemas/delta/date_d.json -o ./include/date_d.hh -n i
avrogencpp -i ./schemas/delta/lineorder_d.json -o ./include/lineorder_d.hh -n i
avrogencpp -i ./schemas/delta/part_d.json -o ./include/part_d.hh -n i
avrogencpp -i ./schemas/delta/supplier_d.json -o ./include/supplier_d.hh -n i