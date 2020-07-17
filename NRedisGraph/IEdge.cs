namespace NRedisGraph
{
    public interface IEdge
    {
        string RelationshipType { get; set; }
        int SourceId { get; set; }
        int DestinationId { get; set; }
    }
}
