import plotly.express as px
import pandas as pd

word_count_str="""
a	12893
b	4862
c	4308
d	2734
e	3491
f	4126
g	1769
h	3099
i	8690
j	342
k	465
l	2699
m	4820
n	2067
o	9720
p	3878
q	173
r	2503
s	7404
t	18613
u	1122
v	935
w	5897
x	32
y	484
z	55
"""

word_count=[l.lstrip().rstrip().split("\t") for l in word_count_str.split("\n") if len(l)>0]
word_count=pd.DataFrame(word_count,columns=["letter","count"])
word_count=word_count.astype({"count":"int32"})
print(word_count)

fig=px.bar(word_count,x="letter",y="count")#,log_y=True)
fig.update_layout(
    autosize=True,
    width=1000,
    height=600,
)
fig.show()
fig.write_image("plot.jpeg")
