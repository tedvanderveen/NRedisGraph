using System.Collections;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;

namespace NRedisGraph
{
    public sealed class ResultSet : IReadOnlyCollection<Record>
    {
        public enum ResultSetScalarType
        {
            VALUE_UNKNOWN,
            VALUE_NULL,
            VALUE_STRING,
            VALUE_INTEGER,
            VALUE_BOOLEAN,
            VALUE_DOUBLE,
            VALUE_ARRAY,
            VALUE_EDGE,
            VALUE_NODE,
            VALUE_PATH
        }

        private readonly RedisResult[] _rawResults;
        private readonly GraphCache _graphCache;

        public ResultSet(RedisResult result, GraphCache graphCache)
        {
            if (result.Type == ResultType.MultiBulk)
            {

                var resultArray = (RedisResult[])result;
                _graphCache = graphCache;

                if (resultArray.Length == 3)
                {
                    Header = new Header(resultArray[0]);
                    Statistics = new Statistics(resultArray[2]);

                    _rawResults = (RedisResult[])resultArray[1];

                    Count = _rawResults.Length;
                }
                else
                {
                    Statistics = new Statistics(resultArray[resultArray.Length - 1]);
                    Count = 0;
                }
            }
            else
            {
                Statistics = new Statistics(result);
                Count = 0;
            }
        }

        public Statistics Statistics { get; }

        public Header Header { get; }

        public int Count { get; }

        public IEnumerator<Record> GetEnumerator() => RecordIterator().GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => RecordIterator().GetEnumerator();

        private IEnumerable<Record> RecordIterator()
        {
            if (_rawResults == default)
            {
                yield break;
            }
            else
            {
                foreach (RedisResult[] row in _rawResults)
                {
                    var parsedRow = new List<object>(row.Length);

                    for (int i = 0; i < row.Length; i++)
                    {
                        var obj = (RedisResult[])row[i];
                        var objType = Header.SchemaTypes[i];

                        switch (objType)
                        {
                            case Header.ResultSetColumnTypes.COLUMN_NODE:
                                parsedRow.Add(DeserializeNode(obj));
                                break;
                            case Header.ResultSetColumnTypes.COLUMN_RELATION:
                                parsedRow.Add(DeserializeEdge(obj));
                                break;
                            case Header.ResultSetColumnTypes.COLUMN_SCALAR:
                                parsedRow.Add(DeserializeScalar(obj));
                                break;
                            default:
                                parsedRow.Add(null);
                                break;
                        }
                    }

                    yield return new Record(Header.SchemaNames, parsedRow);
                }
            }
        }

        private T DeserializeNode<T>(RedisResult[] rawNodeData)
        
        {
            var node = new T((int) rawNodeData[0]);

            var labelIndices = (int[])rawNodeData[1];

            foreach (var labelIndex in labelIndices)
            {
                var label = _graphCache.GetLabel(labelIndex);

                node.AddLabel(label);
            }

            DeserializeGraphEntityProperties(node, (RedisResult[])rawNodeData[2]);

            return node;
        }

        private Edge DeserializeEdge(RedisResult[] rawEdgeData)
        {
            var edge = new Edge((int) rawEdgeData[0])
            {
                RelationshipType = _graphCache.GetRelationshipType((int) rawEdgeData[1]), 
                SourceId = (int) rawEdgeData[2], 
                DestinationId = (int) rawEdgeData[3]
            };

            DeserializeGraphEntityProperties(edge, (RedisResult[])rawEdgeData[4]);

            return edge;
        }

        private object DeserializeScalar(RedisResult[] rawScalarData)
        {
            var type = GetValueTypeFromObject(rawScalarData[0]);

            return type switch
            {
                ResultSetScalarType.VALUE_NULL => null,
                ResultSetScalarType.VALUE_BOOLEAN => bool.Parse((string) rawScalarData[1]),
                ResultSetScalarType.VALUE_DOUBLE => (double) rawScalarData[1],
                ResultSetScalarType.VALUE_INTEGER => (int) rawScalarData[1],
                ResultSetScalarType.VALUE_STRING => (string) rawScalarData[1],
                ResultSetScalarType.VALUE_ARRAY => DeserializeArray((RedisResult[]) rawScalarData[1]),
                ResultSetScalarType.VALUE_NODE => DeserializeNode((RedisResult[]) rawScalarData[1]),
                ResultSetScalarType.VALUE_EDGE => DeserializeEdge((RedisResult[]) rawScalarData[1]),
                ResultSetScalarType.VALUE_PATH => DeserializePath((RedisResult[]) rawScalarData[1]),
                ResultSetScalarType.VALUE_UNKNOWN => (object) rawScalarData[1],
                _ => (object) rawScalarData[1]
            };
        }

        private void DeserializeGraphEntityProperties(GraphEntity graphEntity, RedisResult[] rawProperties)
        {
            foreach (RedisResult[] rawProperty in rawProperties)
            {
                graphEntity.Properties.Add(_graphCache.GetPropertyName((int)rawProperty[0]), DeserializeScalar(rawProperty.Skip(1).ToArray()));
            }
        }

        private object[] DeserializeArray(RedisResult[] serializedArray)
        {
            var result = new object[serializedArray.Length];

            for (var i = 0; i < serializedArray.Length; i++)
            {
                result[0] = DeserializeScalar((RedisResult[])serializedArray[i]);
            }

            return result;
        }

        private Path DeserializePath(RedisResult[] rawPath)
        {
            var nodes = new List<Node>((Node[])DeserializeScalar((RedisResult[])rawPath[0]));
            var edges = new List<Edge>((Edge[])DeserializeScalar((RedisResult[])rawPath[1]));

            return new Path(nodes, edges);
        }

        private static ResultSetScalarType GetValueTypeFromObject(RedisResult rawScalarType) => (ResultSetScalarType)(int)rawScalarType;
    }
}