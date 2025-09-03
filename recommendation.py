import json
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from itertools import combinations

import requests

import re
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Set, Tuple,  Optional
ProductID = str

############# BE data extract

API_BASE = "https://frontend-ec-project-server.onrender.com"
# API_BASE = "http://localhost:5001"

# Shared session (similar to axiosClient)
session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


class CartApi:
    @staticmethod
    def get_all():
        return session.get(f"{API_BASE}/api/carts").json()

    @staticmethod
    def get_by_user_id(user_id):
        return session.get(f"{API_BASE}/api/carts/{user_id}").json()

    @staticmethod
    def create(data):
        return session.post(f"{API_BASE}/api/carts", json=data).json()

    @staticmethod
    def update(cart_id, data):
        return session.put(f"{API_BASE}/api/carts/{cart_id}", json=data).json()

    @staticmethod
    def delete(cart_id):
        return session.delete(f"{API_BASE}/api/carts/{cart_id}").json()

class ProductApi:
    @staticmethod
    def get_all(params=None):
        return session.get(f"{API_BASE}/api/products", params=params).json()

    @staticmethod
    def get_prediction(input_text):
        return session.get(f"{API_BASE}/api/products/predict", params={"search": input_text}).json()

    @staticmethod
    def get_by_id(product_id):
        return session.get(f"{API_BASE}/api/products/{product_id}").json()

    @staticmethod
    def create(data):
        return session.post(f"{API_BASE}/api/products", json=data).json()

    @staticmethod
    def update(product_id, data):
        return session.put(f"{API_BASE}/api/products/{product_id}", json=data).json()

    @staticmethod
    def delete(product_id):
        return session.delete(f"{API_BASE}/api/products/{product_id}").json()


class OrderApi:
    @staticmethod
    def get_all():
        return session.get(f"{API_BASE}/api/orders").json()

    @staticmethod
    def get_by_id(order_id):
        return session.get(f"{API_BASE}/api/orders/{order_id}").json()

    @staticmethod
    def get_by_user_id(user_id):
        return session.get(f"{API_BASE}/api/orders/user/{user_id}").json()

    @staticmethod
    def create(data):
        return session.post(f"{API_BASE}/api/orders", json=data).json()

    @staticmethod
    def update(order_id, data):
        return session.put(f"{API_BASE}/api/orders/{order_id}", json=data).json()
    
def get_all_products():
    total_page = ProductApi.get_all()
    total_page = total_page.get("totalPages", 1)
    all_products = []
    for page in range(0, total_page):
        response = ProductApi.get_all(params={"page": page + 1})
        all_products.extend(response.get("products", []))
    return all_products

def convert_carts():
    '''
    Convert New to Old 
    Old cart structure:
    {
      "user_id": 1, # currently use int, need to modify the code to str
      "products": [
        {
          "product_id": "B1",
          "quantity": 2
        },
        {
          "product_id": "B3",
          "quantity": 3
        }
      ]
    }
    New cart structure:
    {
        "_id": str,
        "user_id": str,
        "items": [], 
            ([
                {
                    "product_id": str,
                    "product_name": str,
                    "quantity": int,
                    "subtotal": float,
                    "off_price": int,
                    "isSelected": bool,
                    "_id": str
                }
            ])
        "createdAt": str,
        "updatedAt": str,
        "__v": int}
    '''
    carts = CartApi.get_all()
    carts_new = []
    for cart in carts:
        new_cart = {
            "user_id": cart["user_id"],  # convert to int
            "products": [
                {
                    "product_id": item["product_id"],
                    "quantity": item["quantity"]
                } for item in cart.get("items", [])
            ]
        }
        carts_new.append(new_cart)
    
    return carts_new

def conver_orders():
    '''
    Convert orders (New) into oders and order_items (Old)
    
    Old orders structure:
    {
      "order_id": 1,
      "user_id": 1,
      "order_date": "02/07/2025",
      "shipping_address": "Q5, TPHCM",
      "total_amount": 59.97,
      "off_price": 0,
      "status": "Required"
    }
    
    Old order_items structure:
    {
      "order_id": 1,
      "products": [
        {
          "product_id": "B1",
          "option": {},
          "price": 0,
          "quantity": 1,
          "off_price": 0
        },
        {
          "product_id": "B2",
          "option": {},
          "price": 0,
          "quantity": 2,
          "off_price": 0
        }
      ]
    }
    
    New orders structure:
    {
        "_id": "68b56cee33d1edae4771ce12",
        "user_id": "68b192b6f62f87ee1a7f23ae",
        "user_name": "Alice",
        "shipping_address": "227",
        "subtotal": 23.27,
        "off_price": 0,
        "status": "Required",
        "payment_method": "Cash on Delivery",
        "message": "",
        "items": [
            {
                "product_id": "68b33a89cddbce7889d9db70",
                "product_name": "Butter Cream",
                "quantity": 1,
                "subtotal": 13.27,
                "off_price": 0,
                "_id": "68b56cdc33d1edae4771cdf4"
            }
        ],
        "createdAt": "2025-09-01T09:52:46.454Z",
        "updatedAt": "2025-09-01T09:52:46.454Z",
        "__v": 0
        }
    '''
    orders = OrderApi.get_all()
    orders_new = []
    order_items_new = []
    for order in orders:
        new_order = {
            "order_id": order["_id"],
            "user_id": order["user_id"],
            "order_date": order["createdAt"][:10],  # extract date part
            "shipping_address": order["shipping_address"],
            "total_amount": order["subtotal"],
            "off_price": order["off_price"],
            "status": order["status"]
        }
        orders_new.append(new_order)
        
        new_order_items = {
            "order_id": order["_id"],
            "products": [
                {
                    "product_id": item["product_id"],
                    "option": {},  # default to empty
                    "price": item["subtotal"] / item["quantity"] if item["quantity"] > 0 else 0,  # calculate unit price
                    "quantity": item["quantity"],
                    "off_price": item["off_price"]
                } for item in order.get("items", [])
            ]
        }
        order_items_new.append(new_order_items)
    return orders_new, order_items_new

def convert_products():
    '''
    Convert New to Old 
    Old product structure:
    {
      "product_id": "B1",
      "type": "flower",
      "name": "Red Rose Bouquet",
      "price": 19.99, # replace by dynamicPrice
      "stock": 12,
      "available": true,
      "description": "A luxurious bouquet of fresh red roses, hand-tied with lush green foliage. Perfect for weddings, anniversaries, or romantic gestures, these premium roses convey love, passion, and elegance. Each stem is carefully selected for vibrant color, long-lasting freshness, and soft velvety petals.",
      "image_url": [
        "/src/assets/demo_1.png",
        "/src/assets/demo.png",
        "/src/assets/demo_2.png",
        "/src/assets/demo_3.png"
      ],
      "flower_details": {
        "occasion": [
          "Wedding"
        ],
        "color": [
          "Pink Flowers"
        ],
        "flower_type": "Roses",
        "options": []
      }
    }
    
    New product structure:
    {
            "_id": "68b33a89cddbce7889d9db70",
            "name": "Butter Cream",
            "price": 10,
            "stock": 10,
            "available": true,
            "stems": 8,
            "description": "A bright mixed Bouq featuring hot pink and yellow roses with pink carnations.",
            "fill_stock_date": "2025-08-30T12:30:00.000Z",
            "sales_count": 8,
            "flower_type": [
                "Roses"
            ],
            "colors": [
                "Yellow Flowers"
            ],
            "occasions": [
                "Birthday",
                "Sympathy"
            ],
            "createdAt": "2025-08-30T17:53:13.155Z",
            "updatedAt": "2025-09-01T16:12:07.717Z",
            "__v": 0,
            "image_url": [
                "/src/assets/Cool%20Breeze.png",
                "/src/assets/Exuberance.png",
                "/src/assets/Flirtatious.png",
                "exuberance",
                "flirtatious",
                "cool_breeze"
            ],
            "dynamicPrice": 11.56,
            "condition": "New arrive"
        }
    '''
    products = get_all_products()
    products_new = []
    for product in products:
        new_product = {
            "product_id": product["_id"],
            "type": "flower",  # default to flower
            "name": product["name"],
            "price": product.get("dynamicPrice", product["price"]),
            "stock": product["stock"],
            "available": product["available"],
            "description": product["description"],
            "image_url": product["image_url"],
            "flower_details": {
                "occasion": product.get("occasions", []),
                "color": product.get("colors", []),
                "flower_type": ", ".join(product.get("flower_type", [])),
                "options": []  # default to empty
            }
        }
        products_new.append(new_product)
    return products_new

def fetch_all_data():
    carts = convert_carts()
    products = convert_products()
    orders, order_items = conver_orders()
    return {
        "carts": carts,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }
    
def extract_to_json():
    data = fetch_all_data()
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

extract_to_json()

############# Utility function

def init_data(data_path = 'data.json'):
    
    # Open the file and load the JSON data
    with open(data_path, 'r') as file:
        data = json.load(file)
        
    # Extract arrays from data
    orders = data.get("orders", [])
    #orders = [order for order in orders if order.get('status','') == 'Done']
    
    orders_id_list = [order['order_id'] for order in orders]
    
    order_items = data.get("order_items", [])
    order_items = [order_item for order_item in order_items if order_item.get('order_id',-1) in orders_id_list]
    
    products = data.get("products", [])
    carts = data.get('carts',[])
    
    return orders, order_items, products, carts


def extract_type(product_ids, products, type_filter = ['flower']):
    
    product_types = []
    # Iterate through each product_id
    for product_id in product_ids:
        # Find the product
        product = next((p for p in products if p["product_id"] == product_id), None)
        if product and product["type"] in type_filter and "flower_details" in product:
            product_types.append({
                'type' : product.get('type',''),
                "flower_type": product["flower_details"].get("flower_type", "N/A"),
                "color": product["flower_details"].get("color", [])
            })
    
    return product_types


def user_data_counter(user_product_types):
    type_count = {} # flower / vase
    flower_count = {} 
    color_count = {}
    
    for product_type in user_product_types:
        #print(product_type)
        type_count[product_type['type']] = type_count.get(product_type['type'], 0) + 1
        if product_type.get('flower_type', '') != '':
            flower_count[product_type['flower_type']] = flower_count.get(product_type['flower_type'], 0) + 1
            for i, color in enumerate(product_type['color']):
                color_count[color] = color_count.get(product_type['color'][i], 0) + 1
      
    type_count = {key: value for key, value in sorted(type_count.items(), key = lambda item: item[1], reverse=True)}      
    flower_count = {key: value for key, value in sorted(flower_count.items(), key = lambda item: item[1], reverse=True)}
    color_count = {key: value for key, value in sorted(color_count.items(), key = lambda item: item[1], reverse=True)}
    
    return type_count, flower_count, color_count

def _is_sellable(pid):
    p = _PRODUCT_BY_ID.get(pid)
    if not p or not p.get("available", True):
        return False
    # if flowers with options, check any option has stock > 0; otherwise use top-level stock
    return p.get("stock", 0) > 0

############# Recommendation History

def get_orders_from_user(user_id, orders ,top_k = 1):
    # Filter orders by user_id
    user_orders = [order for order in orders if order["user_id"] == user_id]
    
    # Sort orders by order_date in descending order
    user_orders.sort(key=lambda x: datetime.strptime(x["order_date"], "%Y-%m-%d"), reverse=True)
    
    # Return the top_k orders (or all if fewer than top_k)
    return user_orders[:top_k]

def get_products_ids_from_orders(orders, order_items):
    product_ids = set()  # Use set for uniqueness
    
    # Iterate through each order
    for order in orders:
        # Find corresponding order_items
        order_item = next((item for item in order_items if item["order_id"] == order["order_id"]), None)
        if order_item:
            # Extract product_ids from the order's products
            for product in order_item["products"]:
                product_ids.add(product["product_id"])
    
    return product_ids

def get_products_from_user_orders(user_id, orders, order_items, products, top_k, type_filter = ['flower']):
    user_orders = get_orders_from_user(user_id, orders, top_k=top_k)
    user_products_ids = get_products_ids_from_orders(user_orders, order_items)
    user_product_types = extract_type(user_products_ids, products, type_filter)
    
    return user_products_ids, user_product_types

from collections import Counter

def rec_user_his(user_id, orders, order_items, products, top_k, recommended_list ,type_filter = ['flower']):
    '''
    Recommend the flower that:
        - User not buy yet
        - Come up with one most refered attribute
        - Come up with the attribute that not occur get_preference
    
    '''
    user_products_ids ,user_product_types = get_products_from_user_orders(user_id, orders, order_items ,products, top_k, type_filter)
    
    
    _, flower_count, color_count = user_data_counter(user_product_types)
    
    # print(f"user_products_ids: {user_products_ids}")
    # print(f"user_product_types: {user_product_types}")
    # print(f"flower_count: {flower_count}")
    # print(f"color_count: {color_count}")
    
    
    #print(flower_count)
    #print(color_count)
    
    # rank signals
    ranked_flower = [ft for ft,_ in Counter(flower_count).most_common()]
    ranked_color  = [c for c,_ in Counter(color_count).most_common()]
    # print(ranked_flower, ranked_color)

    # try same flower_type with a color they haven't bought
    for ft in ranked_flower:
        for p in products:
            if p.get('flower_details',{}).get('flower_type') == ft:
                pid = p['product_id']
                new_colors = [c for c in p.get('flower_details',{}).get('color',[]) if c not in color_count]
                if new_colors and _is_sellable(pid) and pid not in user_products_ids and pid not in recommended_list:
                    recommended_list.add(pid)
                    return {'flag':'history','product_id':pid,'color':new_colors[0],'event':''}, recommended_list

    # fallback: top color they like but new product
    for c in ranked_color:
        for p in products:
            if c in p.get('flower_details',{}).get('color',[]):
                pid = p['product_id']
                if pid not in user_products_ids and _is_sellable(pid) and pid not in recommended_list:
                    recommended_list.add(pid)
                    return {'flag':'history','product_id':pid,'color':c,'event':''}, recommended_list

    # final fallback: any new flower type
    for p in products:
        pid = p['product_id']
        ft = p.get('flower_details',{}).get('flower_type','')
        if ft and ft not in flower_count and _is_sellable(pid) and pid not in recommended_list:
            recommended_list.add(pid)
            return {'flag':'history','product_id':pid,'color':'','event':''}, recommended_list

    return {'flag':'history','product_id':'','color':'','event':''}, recommended_list
        

############# Recommendation Occasion

def rec_user_occasion(products, recommended_list):
    return {
        'flag' : 'occasion',
        'product_id' : '',
        'color' : '',
        'event' : ''
    }, recommended_list

############# Recommendation Cart

def rec_user_cross_selling(user_id, order_items, carts, recommended_list):
    return {
        'flag' : 'cross_selling',
        'product_id': '',
        'color' : '',
        'event': ''
        }, recommended_list

############# Recommendation Best Selling
from collections import defaultdict

def get_products_from_user_carts(user_id, carts):
    cart_item = next((c for c in carts if c["user_id"] == user_id), None)
    return {p["product_id"] for p in cart_item["products"]} if cart_item else set()

def rec_user_best_selling(user_id, order_items, carts, recommended_list):
    """
    Returns up to 3 best-selling product_ids.
    - First pass: obey filters (not in cart, not in recommended_list, sellable).
    - Backfill: ignore cart/recommended filters, but still require sellable.
    - Always returns at least 3 items if possible.
    """
    user_products_id = set(get_products_from_user_carts(user_id, carts))

    # 1) Build revenue per product
    revenue = defaultdict(float)
    for order in order_items:
        for product in order.get('products', []):
            pid = product.get('product_id', '')
            if not pid:
                continue
            opt = product.get('option') or {}
            qty = product.get('quantity', 0) or 0

            # prefer option price if present, else product price
            price = float(opt.get('price', product.get('price', 0.0) or 0.0))
            if qty:
                revenue[pid] += price * qty

    if not revenue:
        return {'flag': 'best', 'product_ids': [], 'color': '', 'event': ''}, recommended_list
    
    # 2) Sort products by revenue desc
    sorted_pids = [pid for pid, _rev in sorted(revenue.items(), key=lambda kv: kv[1], reverse=True)]

    picks = []

    # 3) Primary pass: apply filters
    for pid in sorted_pids:
        if len(picks) == 3:
            break
        if pid in user_products_id:
            continue
        if pid in recommended_list:
            continue
        if not _is_sellable(pid):
            continue
        picks.append(pid)

    # 4) Backfill to ensure 3 (ignore cart/recommended filters, still require sellable)
    if len(picks) < 3:
        for pid in sorted_pids:
            if len(picks) == 3:
                break
            if pid in picks:
                continue
            if not _is_sellable(pid):
                continue
            picks.append(pid)

    # Update recommended_list with everything we return
    recommended_list.update(picks)

    return {'flag': 'best', 'product_ids': picks[:3], 'color': '', 'event': ''}, recommended_list


############# Recommendation wrapper
def rec_user(user_id, top_k=3):
    global recommended_list
    recommended_list = set()  # reset per request/user

    rec_his, recommended_list  = rec_user_his(user_id, orders, order_items, products, top_k,recommended_list)
    rec_cross, recommended_list = rec_user_cross_selling(user_id, order_items, carts,recommended_list)
    rec_occ, recommended_list   = rec_user_occasion(products, recommended_list)
    rec_best, recommended_list  = rec_user_best_selling(user_id, order_items, carts,recommended_list)
    return rec_his, rec_cross, rec_occ, rec_best

def rec_user_converter(user_id, top_k = 3):
    rec_his, rec_cross, rec_occ, rec_best = rec_user(user_id, top_k)
    
    #print(rec_best.get('product_ids', []))
    
    if rec_his.get('product_id', '') == '' and rec_cross.get('product_id','') == '':
        rec_his['product_id'] = rec_best.get('product_ids',[])[1] if len(rec_best.get('product_ids',[])) > 1 else ''
        rec_cross['product_id'] = rec_best.get('product_ids',[])[2] if len(rec_best.get('product_ids',[])) > 2 else ''
        
    if rec_his.get('product_id','') == '':
        rec_his['product_id'] = rec_best.get('product_ids',[])[1] if len(rec_best.get('product_ids',[])) > 1 else ''
        
    if rec_cross.get('product_id','') == '':
        rec_cross['product_id'] = rec_best.get('product_ids',[])[1] if len(rec_best.get('product_ids',[])) > 1 else ''
        
    rec_best['product_id'] = rec_best.get('product_ids',[])[0] if len(rec_best.get('product_ids',[])) > 0 else ''
    
    #print(rec_best['product_ids'])
        
    return rec_his, rec_cross, rec_occ, rec_best

# Data_loader
def load_data(data_path='data.json'):
    global orders, order_items, products, carts, _PRODUCT_BY_ID
    orders, order_items, products, carts = init_data(data_path)
    _PRODUCT_BY_ID = {p["product_id"]: p for p in products}

# call once at module import (keeps current behavior)
load_data()

# also guard the demo print so it doesn't run on import
if __name__ == "__main__":
    print(rec_user_converter('68b192b6f62f87ee1a7f23ae'))
