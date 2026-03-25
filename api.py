from fastapi import FastAPI, Request
import requests
from ortools.constraint_solver import routing_enums_pb2, pywrapcp
import math
import pandas as pd
import os


app = FastAPI()

ACCESS_KEY = os.environ["APPSHEET_KEY"]
APP_ID = os.environ["APPSHEET_APP_ID"]

TABLE_NAME = "test"

ROWIDS = [f"a{100+i}" for i in range(50)]

def dist(a, b):
    return int(((a[0]-b[0])**2 + (a[1]-b[1])**2) * 100000)

def optimize(df):
    coords = list(zip(df["lat"], df["lng"]))
    n = len(coords)

    manager = pywrapcp.RoutingIndexManager(n, 1, 0)
    routing = pywrapcp.RoutingModel(manager)

    def cb(i, j):
        return dist(coords[manager.IndexToNode(i)], coords[manager.IndexToNode(j)])

    t = routing.RegisterTransitCallback(cb)
    routing.SetArcCostEvaluatorOfAllVehicles(t)

    routing.AddDimension(t, 0, 9999999, True, "D")
    dim = routing.GetDimensionOrDie("D")

    for id_val in df["ID"].unique():
        g = df[df["ID"] == id_val]
        if len(g) == 2:
            f = g[g["type"] == "from"].index[0]
            to = g[g["type"] == "to"].index[0]

            fi = manager.NodeToIndex(f)
            ti = manager.NodeToIndex(to)

            routing.AddPickupAndDelivery(fi, ti)
            routing.solver().Add(routing.VehicleVar(fi) == routing.VehicleVar(ti))
            routing.solver().Add(dim.CumulVar(fi) <= dim.CumulVar(ti))

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.time_limit.seconds = 3

    sol = routing.SolveWithParameters(params)

    idx = routing.Start(0)
    order = []
    while not routing.IsEnd(idx):
        order.append(manager.IndexToNode(idx))
        idx = sol.Value(routing.NextVar(idx))

    return order

def write_appsheet(rows):
    url = f"https://api.appsheet.com/api/v2/apps/{APP_ID}/tables/{TABLE_NAME}/Action"
    body = {"Action":"Edit","Rows":rows}
    headers = {"ApplicationAccessKey": ACCESS_KEY}
    requests.post(url, json=body, headers=headers)

@app.post("/optimize")
async def run(req: Request):

    data = await req.json()
    df = pd.DataFrame(data["locations"]).reset_index(drop=True)

    order = optimize(df)
    ordered = [data["locations"][i] for i in order]

    rows = []
    for i, r in enumerate(ordered, 1):
        rows.append({
            "RowID": r["rowId"],
            "順番": i
        })

    write_appsheet(rows)

    return {"ok": True}