from pymongo import MongoClient

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch
db.tbl_pagerank.drop()

db.tbl_pagerank.insert({
    "_id": "A",
    "value": {
        "links": [
            "B",
            "C",
            "D"
        ],
        "pr":   float(1.0/4),
        "length": 4
    }
})

db.tbl_pagerank.insert({
    "_id": "B",
    "value": {
        "links": [
            "A",
            "D",
        ],
        "pr":   float(1.0/4),
        "length": 4
    }
})

db.tbl_pagerank.insert({
    "_id": "C",
    "value": {
        "links": [
            "C",
        ],
        "pr":   float(1.0/4),
        "length": 4
    }
})

db.tbl_pagerank.insert({
    "_id": "D",
    "value": {
        "links": [
            "B",
            "C",
        ],
        "pr":   float(1.0/4),
        "length": 4
    }
})