import jsonlines

data = {}
with jsonlines.open('./jobs_api/jobs.jl','r') as reader:
    for obj in reader.iter(type=dict, skip_invalid=True):
        data[obj['job_id']] = obj

with jsonlines.open('./jobs_api/jobs_nodupes.jl','w') as writer:
    for job_id,obj in data.items():
        writer.write(obj)
