import re, io, sys
p='scripts/build_exog.py'
s=open(p,'r',encoding='utf-8').read()

# (A) 余計な stat_only 行を除去
s=re.sub(r'^[ \t]*stat_only = stat\.drop\(columns=\["unique_id"\], errors="ignore"\)\.reset_index\(drop=True\)\s*\n','',s, flags=re.M)

# (B) concat 内の3番目引数を inline で unique_id 除去版へ
s=re.sub(
    r'(df_out\s*=\s*pd\.concat\(\[df_out,\s*futr\.loc\[df_sorted\.index\]\.reset_index\(drop=True\),\s*)stat\.reset_index\(drop=True\)(\s*\],\s*axis=1\))',
    r'\1stat.drop(columns=["unique_id"], errors="ignore").reset_index(drop=True)\2',
    s, flags=re.M
)

# (C) もし df_out の重複除去行がトップレベル無インデントで入っていたら削除（後段でfeat側で重複除去するため）
s=re.sub(r'^\s*df_out = df_out\.loc\[:, ~df_out\.columns\.duplicated\(\)\]\s*\n','',s, flags=re.M)

# (D) feat = feat[cols] 直後に挿入された行のインデントを揃える
lines=s.splitlines(True)
for i,l in enumerate(lines):
    if 'feat = feat[cols]' in l:
        indent=re.match(r'^(\s*)', l).group(1)
        # 近傍(次の5行)の replace/loc 行を同じインデントへ
        for j in range(i+1, min(i+6, len(lines))):
            if re.search(r'feat\s*=\s*feat\.replace\(\[np\.inf,\s*-np\.inf\],\s*0\)\.fillna\(0\)', lines[j]):
                lines[j]=re.sub(r'^\s*', indent, lines[j])
            if re.search(r'feat\s*=\s*feat\.loc\[:,\s*~feat\.columns\.duplicated\(\)\]', lines[j]):
                lines[j]=re.sub(r'^\s*', indent, lines[j])
        break
s=''.join(lines)
open(p,'w',encoding='utf-8').write(s)
print('patched', p)
