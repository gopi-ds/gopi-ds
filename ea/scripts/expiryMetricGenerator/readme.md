case_holds

````
db.case_items.aggregate([
    {
        "$group": {
            "_id": "$documentKey.gcid",
            "hasHoldFlagTrue": {
                "$max": { "$cond": [{ "$eq": ["$holdFlag", true] }, 1, 0] }
            }
        }
    },
    {
        "$project": {
            "gcid": "$_id",
            "case_hold": {
                "$cond": {
                    "if": { "$eq": ["$hasHoldFlagTrue", 1] },
                    "then": "Y",
                    "else": "N"
                }
            },
            "_id": 0
        }
    }
], { allowDiskUse: true })


````
