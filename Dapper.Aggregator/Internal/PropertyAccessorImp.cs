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
        public abstract bool CanWrite { get; }
        public ColumnAttribute Att { get; set; }
        public string Name { get; set; }
        public abstract object GetValue(object obj);
        public abstract void SetValue(object obj, object value);
        public static PropertyAccessorImp ToAccessor(PropertyInfo pi)
        {
            var getterDelegateType = typeof(Func<,>).MakeGenericType(pi.DeclaringType, pi.PropertyType);
            var getter = Delegate.CreateDelegate(getterDelegateType, pi.GetGetMethod(true));

            Type setterDelegateType = typeof(Action<,>).MakeGenericType(pi.DeclaringType, pi.PropertyType);
            Delegate setter = null;
            var setMethod = pi.GetSetMethod(true);
            if (setMethod != null)
                setter = Delegate.CreateDelegate(setterDelegateType, setMethod);

            var accessorType = typeof(PropertyInfoProvider<,>).MakeGenericType(pi.DeclaringType, pi.PropertyType);
            var provider = (PropertyAccessorImp)Activator.CreateInstance(accessorType, getter, setter, pi);
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
        private readonly Action<TTarget, TProperty> setter;
        private readonly PropertyInfo pInfo;
        public Type PropertyType
        {
            get
            {
                if (propertyType == null)
                    propertyType = typeof(TProperty);
                return propertyType;
            }
        }

        public override bool CanWrite => setter != null;

        private Type propertyType;
        public PropertyInfoProvider(Func<TTarget, TProperty> getter, Action<TTarget, TProperty> setter, PropertyInfo pi)
        {
            this.getter = getter;
            this.setter = setter;
            this.pInfo = pi;
        }
        public override object GetValue(object obj)
        {
            return this.getter((TTarget)obj);
        }
        public override void SetValue(object obj, object value)
        {
            this.setter((TTarget)obj, (TProperty)Convert.ChangeType(value, PropertyType));
        }
    }

    #endregion
}
