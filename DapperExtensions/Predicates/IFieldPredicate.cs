namespace DapperExtensions
{
    public interface IFieldPredicate : IComparePredicate
    {
        object Value { get; set; }
    }
}
