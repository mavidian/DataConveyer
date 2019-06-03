//GlobalCache.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Orchestrators
{
   /// <summary>
   /// Wrapper over ConcurrentDictionary&lt;string, object&gt;, where no values can be added/removed.
   /// It also contains secondary dictionary (signals) to allow thread synchronization.
   /// </summary>
   internal class GlobalCache : IGlobalCache
   {
      private readonly ConcurrentDictionary<string, object> _gcRepository;
      private readonly int _synchronizationInterval;
      private readonly ConcurrentDictionary<string, bool> _signals;

      internal GlobalCache(ConcurrentDictionary<string, object> gcRepository, int synchronizationInterval)
      {
         _gcRepository = gcRepository;
         _synchronizationInterval = synchronizationInterval;
         _signals = new ConcurrentDictionary<string, bool>();  //TOOD: consider Lazy pattern
      }

      public int Count
      {
         get { return _gcRepository.Count;  }
      }

      public IReadOnlyList<KeyValuePair<string, object>> Elements
      {
         get { return _gcRepository.ToList().AsReadOnly();  }
      }


      public object this[string key]
      {
         get
         {
            if (this.TryGet(key, out object retVal)) return retVal;
            throw new ArgumentOutOfRangeException($"There is no '{key}' element in GlobalCache.");
         }
      }

      public bool TryGet(string key, out object value)
      {
         return _gcRepository.TryGetValue(key, out value);
      }

      public bool TryReplace(string key, object newValue, object oldValue)
      {
         //note that we cannot rely on standard comparison as boxed value type objects would've been checked for reference equality
         if (!TryGet(key, out object underlyingValue)) throw new ArgumentOutOfRangeException($"There is no '{key}' element in GlobalCache.");
         if (!AreEqual(underlyingValue, oldValue)) return false;
         return _gcRepository.TryUpdate(key, newValue, underlyingValue);
      }

      public TOut ReplaceValue<TIn, TOut>(string key, Func<TIn, TOut> formula)
      {
         object underlyingVal;
         TOut newVal;
         do
         {
            if (!TryGet(key, out underlyingVal)) throw new ArgumentException($"There is no '{key}' element in GlobalCache.");
            try { newVal = formula((TIn)underlyingVal); }
            catch (Exception ex) { throw new InvalidOperationException($"Exception occurred when calculating '{key}' element value in GlobalCache.", ex); };
         }
         while (!TryReplace(key, newVal, underlyingVal));
         return newVal;
      }


      public int IncrementValue(string key, int increment)
      {
         object underlyingVal;
         int newVal;
         do
         {
            if (!TryGet(key, out underlyingVal)) throw new ArgumentException($"There is no '{key}' element in GlobalCache.");
            if (underlyingVal.GetType() != typeof(int)) throw new ArgumentException($"The GlobalCache element '{key}' is not of integer type.");
            newVal = (int)underlyingVal + increment;
         } while (!_gcRepository.TryUpdate(key, newVal, underlyingVal));
         return newVal;
      }


      public int IncrementValue(string key)
      {
         return IncrementValue(key, 1);
      }


      public void RaiseSignal(string signal)
      {
         _signals.TryAdd(signal, true);
      }


      /// <summary>
      /// Block current thread until a signal is raised.
      /// It is recommended to use the non-blocking <see cref="AwaitSignalAsync"/> method instead.
      /// </summary>
      /// <param name="signal"></param>
      public void AwaitSignal(string signal)
      {
         AwaitSignalAsync(signal).Wait();
      }


      public async Task AwaitSignalAsync(string signal)
      {
         while (!_signals.TryGetValue(signal, out _)) await Task.Delay(_synchronizationInterval);
      }


      /// <summary>
      /// Block current thread until a condition is satisfied.
      /// It is recommended to use the non-blocking <see cref="AwaitConditionAsync"/> method instead.
      /// </summary>
      /// <param name="condition"></param>
      public void AwaitCondition(Func<IGlobalCache, bool> condition)
      {
         AwaitConditionAsync(condition).Wait();
      }


      public async Task  AwaitConditionAsync(Func<IGlobalCache, bool> condition)
      {
         while (!condition(this)) await Task.Delay(_synchronizationInterval);
      }


      private bool AreEqual(object o1, object o2)
      {
         //note that value types are boxed, so == comparison would not work
         if (o1 == null) return o2 == null;
         return o1.Equals(o2);
      }

   }
}
