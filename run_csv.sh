
run_id=7

infolder=data/run${run_id}

folder=data/csv${run_id}
mkdir -p ${folder}

python pgraph/to_csv.py query ${infolder}/az_paper_id.jsonl ${folder}/query.csv
python pgraph/to_csv.py paper_query ${infolder}/az_paper_id.jsonl ${folder}/paper_query.csv
python pgraph/to_csv.py paper ${infolder}/az_paper.jsonl ${folder}/paper.csv

neo4j-admin import --nodes "query.csv" --nodes "paper.csv" --relationships "paper_query.csv" 
