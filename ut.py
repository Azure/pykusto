from column import columnGenerator as Col
from query import Query, Order, Nulls

print(Query().where(Col.foo > 4).take(5).sort_by(Col.bar, Order.ASC, Nulls.LAST).compile_all())
