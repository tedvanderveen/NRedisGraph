using System.Text;
using System.Collections.Generic;

namespace NRedisGraph
{
    public abstract class GraphEntity : IGraphEntity
    {
        protected GraphEntity()
        {
        }

        protected internal GraphEntity(int id)
        {
            Id = id;
        }

        public int Id { get; }

        public override bool Equals(object obj) => this == obj || obj is GraphEntity that && Id == that.Id;

        public override int GetHashCode() => Id;
    }
}