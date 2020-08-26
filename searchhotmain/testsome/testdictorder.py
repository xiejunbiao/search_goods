

a={'a':[1,2,3,4], 'dsc':[4], 'b':[6,3333]}
for k, v in a.items():
    print(k, v)

b={'a':[7,2,3,4], 'dsc':[4], 'b':[6,3333]}
for k, v in b.items():
    print(k, v)


b={'dsc':[4], 'b':[6,3333], 'a':[7,2,3,4]}
for k, v in b.items():
    print(k, v)

d={}
for i in range(4):
    d[str(i)]=[i]
    d['2']=[555]

d['2'].append(333)
print(d)
print(list(d.keys()))

from whoosh.query import Term
a=Term("content", u"a") | Term("content", u"b")
print(a)

