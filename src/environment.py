import simpy
import random
import pandas as pd
import numpy as np
import yaml
from pathlib import Path

# Read config file
# products demand
products_demand_path = Path("config/products_demand.yaml")
with open(products_demand_path, 'r') as file:
    products_demand = yaml.safe_load(file)

# process flow
processes_path = Path("config/process_flow.yaml")
with open(processes_path, 'r') as file:
    processes = yaml.safe_load(file)

# resources config
resources_config_path = Path("config/resources_config.yaml")
with open(resources_config_path, 'r') as file:
    resources_config = yaml.safe_load(file)

# time config
config_path = Path("config/config.yaml")
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

time_unit = config["time_unit"]
time_on = config["time_on"]
time_off = config["time_off"]
schedule_time = config["schedule_time"]
schedule_mode = config["schedule_mode"]
delivery_time = config["delivery_time"]

if time_unit == 'hour':
    cicle = 24
elif time_unit == 'minute':
    cicle = 24*60

def generate_random_number(distribution, params):

    if distribution == "constant":
        return params[0]
    elif distribution == "expo":
        return int(random.expovariate(1/params[0]))
    elif distribution == "normal":
        return int(random.normalvariate(params[0], params[1]))

def generate_day_demand(actual_time):
    global DEMAND

    for product in products_demand:
        generate_dist = products_demand[product]["generate"]["dist"]
        generate_params = products_demand[product]["generate"]["params"]
        duedate_dist = products_demand[product]["duedate"]["dist"]
        duedate_params = products_demand[product]["duedate"]["params"]
        # Create product df line
        quantity = generate_random_number(generate_dist,
                                         generate_params)
        duedate = generate_random_number(duedate_dist,
                                         duedate_params)
        if quantity == 0:
            pass
        else:
            day_demand = {
                "created_at": [actual_time],
                "product": [product],
                "quantity": [quantity],
                "due_date": [actual_time + duedate],
                "delivered_at": [0]
            }
            day_demand = pd.DataFrame(day_demand)
            DEMAND = pd.concat([DEMAND, day_demand], ignore_index=True)
            

def get_day_hour(time, cicle):
    actual = time / cicle
    day = int(actual)
    hour = (actual - day) * 24
    return day, int(round(hour))


# --- ENVIRONMENT ----

def create_resources(env, resources_config):
    resource_dict = {}
    for resource in resources_config:
        capacity = resources_config[resource]["capacity"]
        # create resource
        resource_dict[resource] = simpy.Resource(env, capacity)

    return resource_dict

def create_container(env, products_demand):
    container_dict = {}
    for product in products_demand:
        container_dict[product] = simpy.Container(env, init=0)
    
    return container_dict

def scheduler():
    global DEMAND
    global ORDERS

    print(DEMAND)
    if schedule_mode == "direct":
        order_id = len(ORDERS)
        
        for idx in DEMAND.index:
            orders_list = [
                order_id,        
                DEMAND.loc[idx, "product"],
                DEMAND.loc[idx, "quantity"],
                "",
                ""
            ]
            
            # orders_list = np.reshape(orders_list, (len(orders_list), 1))
            print(orders_list)
            ORDERS.loc[len(ORDERS)] = orders_list
            order_id += 1
    print(ORDERS)


def orders_dispatch(env, resources, stocks):

    open_orders = ORDERS.loc[ORDERS["dispatched"] == ""].index
    for idx in open_orders:
        quantity = ORDERS.loc[idx, "quantity"]
        product = ORDERS.loc[idx, "product"]
        
        ORDERS.loc[idx, "dispatched"] = env.now
        for unit in range(0, quantity):
            
            env.process(manufacture(env, unit, idx, product, resources, stocks))


def manufacture(env, unit, idx, product, resources, stocks):
    global REPORT
    
    order_report = [idx, product, unit]
    
    product_process = processes[product]
    
    for resource in product_process:
        
        with resources[resource].request() as req:

            order_report.append(env.now)
            
            yield req
            
            order_report.append(env.now)
            
            dist = product_process[resource]["process_time"]["dist"]
            params = product_process[resource]["process_time"]["params"]
            processing_time = generate_random_number(dist, params)

            yield env.timeout(processing_time)
            order_report.append(env.now)
            
    stocks[product].put(1)
    
    order_report.append(env.now)
    # order_report = np.reshape(order_report,len(order_report),1)
    REPORT.loc[len(REPORT)] = order_report
    print(REPORT)

def delivery(env, stocks):

    open_demand = DEMAND.loc[DEMAND["delivered_at"] == 0].sort_values("due_date", ascending=False)

    for id in open_demand.index:
        product = DEMAND.loc[id, "product"]
        quantity = DEMAND.loc[id, "quantity"]
        

        if stocks[product].level >= quantity:
            stocks[product].get(quantity)
            DEMAND.loc[id, "delivered_at"] = env.now
        
def controller(env):
    while True:
        now = env.now
        day, hour = get_day_hour(now, cicle)
        # print(f'{now} - H: {hour}')

        if hour == 0:
            generate_day_demand(now)
            
        elif hour == schedule_time:

            scheduler()
            orders_dispatch(env, resources, stocks)

        elif hour == delivery_time:

            delivery(env, stocks)
            # print(demand)
        yield env.timeout(1)

        # elif hour >= time_on and hour <= time_off:

        #     if not working.triggered:
        #         working.succeed()
        
        # elif hour > time_off:
        #     working = simpy.Event(env)
        
        # yield env.timeout(1)



# Create DEMAND DF
demand_columns = [
    "created_at",
    "product",
    "quantity",
    "due_date",
    "delivered_at"
    ]
DEMAND = pd.DataFrame(columns=demand_columns)

# Create orders DF
orders_columns = [
    "order_id",
    "product",
    "quantity",
    "dispatched",
    "finished"
    ]
ORDERS = pd.DataFrame(columns=orders_columns)

# Create production report
prod_report_columns = [
    "order_id",
    "product",
    "unit"
]
for resource in resources_config:
    prod_report_columns.append(f'{resource}_qu')
    prod_report_columns.append(f'{resource}_st')
    prod_report_columns.append(f'{resource}_fi')
prod_report_columns.append("stocked")
REPORT = pd.DataFrame(columns=prod_report_columns)

# Simulation start
env = simpy.Environment()
working = simpy.Event(env)

# Create elements
resources = create_resources(env, resources_config)
stocks = create_container(env, products_demand)

# Start events
env.process(controller(env))
# env.process(production(env, resources, orders))

env.run(until=50)



