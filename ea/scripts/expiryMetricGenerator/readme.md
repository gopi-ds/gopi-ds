case_holds

````
db.case_items.aggregate([
  {
    $group: {
      _id: {
        gcid: "$documentKey.gcid",
        holdFlag: "$holdFlag"
      },
      count: { $sum: 1 }
    }
  },
  {
    $group: {
      _id: "$_id.gcid",
      hasHoldFlagTrue: {
        $sum: { $cond: [{ $eq: ["$_id.holdFlag", true] }, "$count", 0] }
      },
      hasHoldFlagFalse: {
        $sum: { $cond: [{ $eq: ["$_id.holdFlag", false] }, "$count", 0] }
      }
    }
  },
  {
    $project: {
      gcid: "$_id",
      case_hold: {
        $cond: {
          if: { $gt: ["$hasHoldFlagTrue", "$hasHoldFlagFalse"] },
          then: "Y",
          else: "N"
        }
      },
      _id: 0
    }
  }
], { allowDiskUse: true })

````
