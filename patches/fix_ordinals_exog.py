import re
p='scripts/build_exog.py'
s=open(p,'r',encoding='utf-8').read()

# grng["stat_start_ord"] / grng["stat_end_ord"] を days since epoch (int32) に
s=re.sub(
    r'grng\["stat_start_ord"\]\s*=\s*grng\["min"\]\.astype\("int64"\)',
    'grng["stat_start_ord"] = ((grng["min"].dt.floor("D") - pd.Timestamp("1970-01-01")).dt.days.astype("int32"))',
    s
)
s=re.sub(
    r'grng\["stat_end_ord"\]\s*=\s*grng\["max"\]\.astype\("int64"\)',
    'grng["stat_end_ord"]   = ((grng["max"].dt.floor("D") - pd.Timestamp("1970-01-01")).dt.days.astype("int32"))',
    s
)

open(p,'w',encoding='utf-8').write(s)
print("patched", p)
