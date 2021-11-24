using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    [Serializable]
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class RelationAttribute : Attribute
    {
        public RelationAttribute(Type childType, string parentPropertyName, string childPropertyName)
            : this(childType, string.Empty, parentPropertyName, childPropertyName)
        { }

        public RelationAttribute(Type childType, string key, string parentPropertyName, string childPropertyName)
            : this(childType, key, new string[] { parentPropertyName }, new string[] { childPropertyName })
        { }

        public RelationAttribute(Type childType, string[] parentPropertyName, string[] childPropertyName)
            : this(childType, string.Empty, parentPropertyName, childPropertyName)
        { }

        public RelationAttribute(Type childType, string key, string[] parentPropertyName, string[] childPropertyName)
            : this(null, childType, key, parentPropertyName, childPropertyName)
        { }

        public RelationAttribute(Type parentType, Type childType, string parentPropertyName, string childPropertyName)
            : this(parentType, childType, string.Empty, new[] { parentPropertyName }, new[] { childPropertyName })
        { }

        public RelationAttribute(Type parentType, Type childType, string[] parentPropertyName, string[] childPropertyName)
            : this(parentType, childType, string.Empty, parentPropertyName, childPropertyName)
        { }

        public RelationAttribute(Type parentType, Type childType, string key, string[] parentPropertyName, string[] childPropertyName)
        {
            ParentType = parentType;
            ChildType = childType;
            ParentPropertyNames = parentPropertyName;
            ChildPropertyNames = childPropertyName;
            Key = key;
        }
        public Type ParentType { get; internal set; }
        public Type ChildType { get; private set; }
        public string[] ParentPropertyNames { get; private set; }
        public string[] ChildPropertyNames { get; private set; }
        public string Key { get; private set; }
        public string ParentTableName { get; private set; }
        public string ParentAliasTableName { get; private set; }
        public string ChildTableName { get; private set; }
        public string ChildAliasTableName { get; private set; }

        //[NonSerialized]
        internal List<PropertyAccessorImp> parentPropertyAccessors = new List<PropertyAccessorImp>();
        //[NonSerialized]
        internal List<PropertyAccessorImp> childPropertyAccessors = new List<PropertyAccessorImp>();

        internal bool Loaded { get; set; }
        public void Ensure(QueryImp query)
        {
            if (string.IsNullOrWhiteSpace(Key))
            {
                Key = CreateDefaultKey(ParentType, ChildType);
            }
            ParentTableName = ParentType.GetTableName();
            ParentAliasTableName = query.EscapeAliasFormat(ParentTableName);
            ChildTableName = ChildType.GetTableName();
            ChildAliasTableName = query.EscapeAliasFormat(ChildTableName);

            var parentProperties = PropertyAccessorImp.ToPropertyAccessors(ParentType);
            var childProperties = PropertyAccessorImp.ToPropertyAccessors(ChildType);
            for (int i = 0; i < ParentPropertyNames.Length; i++)
            {
                var pName = ParentPropertyNames[i];
                var cName = ChildPropertyNames[i];

                var ppi = parentProperties.Single(x => x.Att.PropertyInfoName == pName);
                var cpi = childProperties.Single(x => x.Att.PropertyInfoName == cName);

                parentPropertyAccessors.Add(ppi);
                childPropertyAccessors.Add(cpi);
            }
            DataAdapter = new DapperDataAdapter(this);
        }
        public void EnsureDynamicType()
        {
            if (!ParentType.GetInterfaces().Any(x => x == typeof(IContainerHolder)))
            {
                ParentType = ILGeneratorUtil.InjectionInterfaceWithProperty(ParentType);
            }
            if (!ChildType.GetInterfaces().Any(x => x == typeof(IContainerHolder)))
            {
                ChildType = ILGeneratorUtil.InjectionInterfaceWithProperty(ChildType);
            }
        }

        [NonSerialized]
        internal DapperDataAdapter DataAdapter;
        static string CreateDefaultKey(Type parentType, Type childType)
        {
            return string.Format("{0}.{1}", parentType.Name, childType.Name);
        }
    }

}
