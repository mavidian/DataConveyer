//IGlobalCache.cs
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
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Orchestrators
{
   /// <summary>
   /// <para>
   /// Interface that defines a GlobalCache, a thread-safe repository of key value pairs to contain global data and signals to
   /// synchronize multi-threaded processing.
   /// GlobalCache elements are available throughout all phases of Data Conveyer processing.
   /// Element keys are of type string, values can be one of: int, DataTime, decimal, string or object.
   /// </para>
   /// <para>
   /// GlobalCache elements must be defined using <see cref="OrchestratorConfig.GlobalCacheElements"/> configuration setting.
   /// During the process execution, element values can be updated, but no elements can be added/removed.
   /// </para>
   /// <para>
   /// Signals, unlike elements, do not need to be declared; instead they are simply referred to in <see cref="RaiseSignal"/>,
   /// <see cref="AwaitSignal"/>and <see cref="AwaitSignalAsync"/> methods.
   /// </para>
   /// <note type="caution">
   /// GlobalCache is exposed to concurrent access by multiple threads and therefore updates to its elements
   /// (such as <see cref="TryReplace">TryReplace</see> method) must be performed in a thread-safe manner.
   /// Data Conveyer makes no guarantees regarding a particular processing order. As a consequence, user code may need
   /// to address possible race conditions, for example by using <see cref="AwaitCondition"/> method.
   /// </note>
   /// </summary>
   public interface IGlobalCache
   {
      //TODO: Consider enhancing the interface by adding action parameter to Await.. methods, so that the action is performed
      //      in the same synchronization context once the await condition is met.

      /// <summary>
      /// Number of elements held. This value remains constant throughout execution of Data Conveyer process.
      /// </summary>
      int Count { get; }
      /// <summary>
      /// A collection of all elements held.
      /// </summary>
      IReadOnlyList<KeyValuePair<string, object>> Elements { get; }
      /// <summary>
      /// Retrieve the current value for a given key.
      /// </summary>
      /// <param name="key">Key of the value to retrieve.</param>
      /// <exception cref="ArgumentOutOfRangeException">No element for a given key is present in GlobalCache.</exception>
      /// <returns>Current value for a given key.</returns>
      object this[string key] { get; }
      /// <summary>
      /// Attempt to retrieve the current value for a given key.
      /// </summary>
      /// <param name="key">Key of the value to retrieve.</param>
      /// <param name="value">Retrieved value.</param>
      /// <returns>True if the value was retrieved; false if the key is not present in GlobalCache.</returns>
      bool TryGet(string key, out object value);
      /// <summary>
      /// Attempt to replace the value for a given key. This method is thread-safe.
      /// Note that there is no guarantee that any call to <see cref="TryReplace">TryReplace</see> method will succeed; it will fail (return false) if another thread changed the underlying value (oldValue).
      /// To guarantee a successful replacement, <see cref="TryGet">TryGet</see> and <see cref="TryReplace">TryReplace</see> methods need to be called in a loop until <see cref="TryReplace">TryReplace</see> returns true.
      /// Alternatively, <see cref="ReplaceValue">ReplaceValue</see> method may be called, which executes such loop internally.
      /// </summary>
      /// <param name="key">Key of the value to replace.</param>
      /// <param name="newValue">New value to be placed in GlobalCache for the key.</param>
      /// <param name="oldValue">Current value for the key (to be compared against the underlying value in GlobalCache).</param>
      /// <exception cref="ArgumentOutOfRangeException">No element for a given key is present in GlobalCache.</exception>
      /// <returns>True if the value was successfully replaced; false if the oldValue didn't match the underlying value in GlobalCache and hence no replacement took place.</returns>
      bool TryReplace(string key, object newValue, object oldValue);
      /// <summary>
      /// Replace value for a given key based on a given calculation formula.
      /// </summary>
      /// <typeparam name="TIn">Type of the value to replace (i.e. the old value).</typeparam>
      /// <typeparam name="TOut">Type of the replacement value (i.e. the new value).</typeparam>
      /// <param name="key">Key of the value to replace.</param>
      /// <param name="formula">Function to calculate a new value from the old value.</param>
      /// <exception cref="ArgumentException">Element for a given key does not exist or is of type other than TIn.</exception>
      /// <exception cref="InvalidOperationException">Formula threw exception during calculation; see InnerException for details.</exception>
      /// <returns>New value that replaced the old value.</returns>
      TOut ReplaceValue<TIn, TOut>(string key, Func<TIn, TOut> formula);
      /// <summary>
      /// Add a given number to an integer value held in GlobalCache. This method is thread-safe.
      /// </summary>
      /// <param name="key">Key of the value to increment.</param>
      /// <param name="increment">Value to increment by.</param>
      /// <exception cref="ArgumentException">Element for a given key does not exist or is of type other than int.</exception>
      /// <returns>New (incremented) value.</returns>
      int IncrementValue(string key, int increment);
      /// <summary>
      /// Add 1 to an integer value held in GlobalCache. This method is thread-safe.
      /// </summary>
      /// <param name="key">Key of the value to increment by 1.</param>
      /// <returns>New (incremented) value.</returns>
      /// <exception cref="ArgumentException">Element for a given key does not exist or is of type other than int.</exception>
      int IncrementValue(string key);
      /// <summary>
      /// Raise a signal that another part (thread) of DataConveyer process might be waiting for. Once this method is called, any method held by
      /// either the <see cref="AwaitSignal"/> or <see cref="AwaitSignalAsync"/> method will continue execution.
      /// </summary>
      /// <param name="signal">Name of the signal to raise.</param>
      void RaiseSignal(string signal);
      /// <summary>
      /// Wait for a given signal to be raised before returning. This method blocks the current thread until the given signal is raised.
      /// Therefore, special care must be taken to assure that the <see cref="RaiseSignal"/> method is called on another thread, so that
      /// a deadlock condition is avoided.
      /// </summary>
      /// <param name="signal">Name of the signal to await.</param>
      void AwaitSignal(string signal);
      /// <summary>
      /// Asynchronously wait for a given signal to be raised. The returned task will not complete until the given signal is raised using the
      /// <see cref="RaiseSignal"/> method. When called from an asynchronous method, this method allows synchronization of DataConveyer processing
      /// without thread blocking.
      /// </summary>
      /// <param name="signal">Name of the signal to await.</param>
      /// <returns>A task intended to be awaited for in the client code.</returns>
      Task AwaitSignalAsync(string signal);
      /// <summary>
      /// Wait for a given condition to be satisfied before returning. This method blocks the current thread until the provided function evaluates
      /// to true. Therefore, special care must be taken when defining the function (condition parameter) to avoid a deadlock condition.
      /// </summary>
      /// <param name="condition">A predicate expected to return true once the condition is met.</param>
      void AwaitCondition(Func<IGlobalCache,bool> condition);
      /// <summary>
      /// Asynchronously wait for a given condition to be satisfied. The returned task will not complete until the provided function evaluates
      /// to true. When called from an asynchronous method, this method allows synchronization of DataConveyer processing without thread blocking.
      /// </summary>
      /// <param name="condition">A predicate expected to return true once the condition is met.</param>
      /// <returns>A task intended to be awaited for in the client code.</returns>
      Task AwaitConditionAsync(Func<IGlobalCache, bool> condition);
   }
}
