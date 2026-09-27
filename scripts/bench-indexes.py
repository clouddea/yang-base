#!/usr/bin/env python3
"""运行索引性能门禁；CSV 凭证仅经环境传给 Gradle，不打印或写入报告。"""
import argparse
import csv
import os
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend', choices=['embedded', 'minio', 'oss'], default='embedded')
    parser.add_argument('--endpoint')
    parser.add_argument('--bucket')
    parser.add_argument('--region')
    parser.add_argument('--access-key-csv', type=Path)
    parser.add_argument('--rows', type=int, default=6000)
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.rows < 4000 or args.rows % 4 or args.samples < 1:
        parser.error('rows 必须 >=4000 且能被 4 整除；samples 必须 >=1')
    if args.backend == 'oss' and not args.bucket:
        parser.error('OSS 必须显式指定 --bucket')
    if args.backend == 'embedded' and (args.access_key_csv or args.endpoint or args.bucket):
        parser.error('embedded 不接受外部存储配置')
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    # 防止 ambient 环境把 embedded 验收意外送到外部桶。
    for key in list(env):
        if key.startswith('YB_S3_'):
            del env[key]
    if args.backend != 'embedded':
        if args.access_key_csv:
            with args.access_key_csv.open(encoding='utf-8-sig', newline='') as source:
                rows = list(csv.DictReader(source))
            if len(rows) != 1:
                parser.error('AccessKey CSV 必须恰好包含一组凭证')
            row = {''.join(c for c in k.lower() if c.isalnum()): v.strip() for k, v in rows[0].items()}
            ak, sk = row.get('accesskeyid'), row.get('accesskeysecret')
        else:
            ak, sk = os.getenv('YB_S3_ACCESS_KEY'), os.getenv('YB_S3_SECRET_KEY')
            if args.backend == 'minio':
                ak = ak or os.getenv('MINIO_ROOT_USER', 'yangbase')
                sk = sk or os.getenv('MINIO_ROOT_PASSWORD', 'yangbase123')
        if not ak or not sk:
            parser.error('需要 --access-key-csv 或 YB_S3_ACCESS_KEY / YB_S3_SECRET_KEY 环境变量')
        env.update(YB_S3_ACCESS_KEY=ak, YB_S3_SECRET_KEY=sk)
    report = (args.report or root / 'yb-bench/build/reports/index-bench' / f'{args.backend}.tsv').resolve()
    command = [str(root / 'gradlew'), ':yb-bench:benchIndexes', '--console=plain', '--no-build-cache',
               f'-PYB_INDEX_ROWS={args.rows}', f'-PYB_INDEX_SAMPLES={args.samples}', f'-PYB_INDEX_REPORT={report}']
    if args.backend != 'embedded':
        oss = args.backend == 'oss'
        region = args.region or ('cn-hangzhou' if oss else 'us-east-1')
        endpoint = args.endpoint or (f'https://oss-{region}.aliyuncs.com' if oss else 'http://127.0.0.1:9000')
        command += [f'-PYB_S3_ENDPOINT={endpoint}', f'-PYB_S3_BUCKET={args.bucket or "yangbase-index-bench"}',
                    f'-PYB_S3_REGION={region}', f'-PYB_S3_OSS={str(oss).lower()}',
                    f'-PYB_S3_PATH_STYLE={str(not oss).lower()}']
    report.parent.mkdir(parents=True, exist_ok=True)
    metadata = dict(backend=args.backend, rows_per_table=args.rows, samples=args.samples,
                    command=command, started_at=datetime.now(timezone.utc).isoformat(),
                    cache="data cache disabled; metadata warmed; fixed read-only snapshot",
                    latency="descriptive only; three samples by default, not a latency SLO",
                    git_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip())
    result = subprocess.run(command, cwd=root, env=env, check=False)
    metadata.update(exit_code=result.returncode, finished_at=datetime.now(timezone.utc).isoformat())
    report.with_suffix('.json').write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
