| Phase | Task | Owner | Start | End | Status | Overall | Risk | NextAction |
|---|---|---|---|---|---:|---:|---|---|
| P1 | 不具合修正(ConfigDict/DB init) | az | $(date +%Y-%m-%d) | $(date +%Y-%m-%d) | 100 | 20 | 影響範囲の再テスト | cli run の再実行とDB接続検証 |
| P1 | 構造/関係インベントリ(JSON化) | az | $(date +%Y-%m-%d) | $(date +%Y-%m-%d) | 100 | 35 | 解析粒度不足 | 依存グラフの可視化追加 |
| P1 | Dry-run（データ→FE→推論） | az | $(date +%Y-%m-%d) | TBD | 0 | 35 | 性能ばらつき | ログに基づき所要時間見積 |
| P1 | 計測設計（メトリクス/ログ配置） | az | $(date +%Y-%m-%d) | TBD | 0 | 35 | メトリクス未定義 | MLflow/DB/ファイルの責務整理 |
| P1 | 計画レビュー/合意 | az | $(date +%Y-%m-%d) | TBD | 0 | 35 | ステークホルダ調整 | レビュー用抜粋資料作成 |
