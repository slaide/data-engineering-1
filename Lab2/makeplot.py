import plotly.express as px
import pandas as pd

word_count_str="""
den 1220703 (52.131662% of total)
denna 22475 (0.959823% of total)
denne 3988 (0.170313% of total)
det 456539 (19.497074% of total)
han 666483 (28.462997% of total)
hen 31150 (1.330300% of total)
hon 307084 (13.114410% of total)
"""

word_count=[l.lstrip().rstrip().split(" ")[:3:2] for l in word_count_str.split("\n") if len(l.rstrip().lstrip())>0]
word_count=[(w,float(c[1:-1])) for (w,c) in word_count]
x_colname="pronoun"
y_colname="percent of total tweets"
word_count=pd.DataFrame(word_count,columns=[x_colname,y_colname])
word_count=word_count.astype({y_colname:"float32"})
print(word_count)

fig=px.bar(word_count,x=x_colname,y=y_colname)
fig.update_layout(
    autosize=True,
    width=1000,
    height=600,
)
fig.show()
fig.write_image("plot.jpeg")
