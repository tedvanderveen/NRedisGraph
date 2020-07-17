using System.Linq;
using System.Text;

namespace NRedisGraph
{
    public class Edge<TSource, TDestination> : GraphEntity, IEdge where TSource : IGraphEntity
        where TDestination : IGraphEntity
    {
        public Edge()
        {
        }

        internal Edge(int id) : base(id)
        {
        }

        public string RelationshipType { get; set; }

        public int SourceId { get; set; }

        public int DestinationId { get; set; }
    }
}