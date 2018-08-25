
run_id=7

folder=data/run${run_id}
mkdir -p ${folder}


# python pgraph/gs_scrapper.py input/keywords_phd.csv ${folder}/gs.jsonl 2014 3 social\ networks
# python pgraph/az_paper.py ${folder}/gs.jsonl ${folder}/az_paper_id.jsonl
# python pgraph/az_graph.py p4p ${folder}/az_paper_id.jsonl ${folder}/az_paper.jsonl
# python pgraph/az_graph.py a4p ${folder}/az_paper.jsonl ${folder}/az_paper_author.jsonl
# python pgraph/az_graph.py p4a ${folder}/az_paper_author.jsonl ${folder}/az_paper_author_paper.jsonl
# python pgraph/az_graph.py aff4p ${folder}/az_paper_author_paper.jsonl ${folder}/az_paper_author_paper_affiliation.jsonl
python pgraph/google_location.py ${folder}/az_paper_author_paper_affiliation.jsonl ${folder}/az_paper_author_paper_affiliation_location.jsonl
