using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;

namespace NRedisGraph
{
    public abstract class Node : GraphEntity
    {
        protected Node()
        {
        }

        internal Node(int id) : base(id)
        {
        }
    }
}