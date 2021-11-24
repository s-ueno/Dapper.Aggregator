using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    #region not use reflection. simple GetGetMethod access

    [Serializable]
    internal abstract class PropertyAccessorImp
    {
        public ColumnAttribute Att { get; set; }
        public string Name { get; set; }
        public abstract object GetValue(object obj);
        public static PropertyAccessorImp ToAccessor(PropertyInfo pi)
        {
            var getterDelegateType = typeof(Func<,>).MakeGenericType(pi.DeclaringType, pi.PropertyType);
            var getter = Delegate.CreateDelegate(getterDelegateType, pi.GetGetMethod(true));
            var accessorType = typeof(PropertyInfoProvider<,>).MakeGenericType(pi.DeclaringType, pi.PropertyType);
            var provider = (PropertyAccessorImp)Activator.CreateInstance(accessorType, getter);
            provider.Name = pi.Name;
            provider.Att = pi.CreateColumnInfo();
            return provider;
        }

        public static IEnumerable<PropertyAccessorImp> ToPropertyAccessors(Type t)
        {
            List<PropertyAccessorImp> result;
            if (!dic.TryGetValue(t, out result))
            {
                var list = t.GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
                            .Where(x => x.CanRead)
                            .Select(x =>
                            {
                                var accessor = ToAccessor(x);
                                accessor.Att = x.CreateColumnInfo();
                                return accessor;
                            })
                            .ToList();
                result = dic[t] = list;
            }
            return result;
        }
        private static ConcurrentDictionary<Type, List<PropertyAccessorImp>> dic = new ConcurrentDictionary<Type, List<PropertyAccessorImp>>();

    }
    [Serializable]
    internal class PropertyInfoProvider<TTarget, TProperty> : PropertyAccessorImp
    {
        private readonly Func<TTarget, TProperty> getter;
        public PropertyInfoProvider(Func<TTarget, TProperty> getter)
        {
            this.getter = getter;
        }
        public override object GetValue(object obj)
        {
            return this.getter((TTarget)obj);
        }
    }

    #endregion
}
