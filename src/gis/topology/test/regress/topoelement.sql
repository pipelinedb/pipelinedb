-- Good
SELECT '1','{101,1}'::topology.TopoElement;
SELECT '2','{101,2}'::topology.TopoElement;
SELECT '3','{101,3}'::topology.TopoElement;
SELECT '4','{1,104}'::topology.TopoElement; -- layer id has no higher limit
-- Invalid: has 3 elements
SELECT '[0:2]={1,2,3}'::topology.TopoElement;
-- Invalid: 0 is both an invalid primitive element id and an invalid layer id
SELECT '{1,0}'::topology.TopoElement;
