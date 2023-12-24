import simpy
import random
import pandas as pd

def part_generator(env, resources, processing_times):
    global db
    part_id = 0
    while True:
        part = flow_generator(env, part_id, resources, processing_times)
        
        env.process(part)
        
        part_arrived_time = env.now
        
        print(f'{part_arrived_time} - Part arrived - id: {part_id}')
        
        data = [part_id]
        for resource in resources:
            data.append(0)
            data.append(0)
            data.append(0)
        db.loc[len(db)] = data
        
        t = int(random.normalvariate(4,1))

        yield env.timeout(t)
        part_id += 1


def flow_generator(env, part_id, resources, processing_times):

    global db

    for i, resource in enumerate(resources):

        part_queued_time = env.now
    
        print(f'{part_queued_time} - Part queued - rec: {resource}id: {part_id}')
        db.loc[db["id"] == part_id, f'queued_{resource}'] = part_queued_time

        with resources[resource].request() as req:
            yield req

            part_start_process_time = env.now
            print(f'{part_start_process_time} - Part start process - rec:{resource} id:{part_id}')
            db.loc[db["id"] == part_id, f'started_{resource}'] = part_start_process_time
    
            
            mu = processing_times[i][0]
            sigma = processing_times[i][1] 
            processing_time = int(random.normalvariate(mu, sigma))

            yield env.timeout(processing_time)

            part_processed_time = env.now
            print(f'{part_processed_time} - Part processed - rec:{resource} id:{part_id}')
            db.loc[db["id"] == part_id, f'finished_{resource}'] = part_processed_time
    

def create_resources(env, resources):
    resource_dict = {}
    for resource in resources:
        resource_dict[resource] = simpy.Resource(env, capacity=1)

    return resource_dict


env = simpy.Environment()

resources = ["p1", "p2"]
processing_times = [[5,1],[7,2]]

db_columns = ["id"]
for resource in resources:
    db_columns.append(f'queued_{resource}')
    db_columns.append(f'started_{resource}')
    db_columns.append(f'finished_{resource}')
db = pd.DataFrame(columns = db_columns)

resources = create_resources(env, resources)

env.process(part_generator(env, resources, processing_times))

env.run(until=200)

print(db)

