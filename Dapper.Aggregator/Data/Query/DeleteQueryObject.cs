namespace Dapper.Aggregator
{
    internal class DeleteQueryObject
    {
        public DeleteQueryObject(RelationAttribute att, string view, int nestLevel)
        {
            this.Criteria = new NestCriteria(att) { View = view };
            NestLevel = nestLevel;
        }
        public NestCriteria Criteria { get; set; }
        public int NestLevel { get; set; }
        public string DeleteClause
        {
            get
            {
                return string.Format("DELETE FROM {0} WHERE {1}", Criteria.Att.ChildTableName, Criteria.BuildStatement());
            }
        }
        public string View
        {
            get
            {
                return string.Format("SELECT {0} FROM {1} WHERE {2}", Criteria.Att.ChildType.GetSelectClause().ToSelectClause(), Criteria.Att.ChildTableName, Criteria.BuildStatement());
            }
        }
    }

}
