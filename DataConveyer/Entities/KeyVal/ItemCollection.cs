//ItemCollection.cs
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


using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// Collection of KeyValItem objects in KeyValRecord class; allows element access by either a key (string) or an index (number).
   /// </summary>
   internal class ItemCollection : KeyedCollection<string, IItem>
   {
      // KeyedCollection assures ordered iteration of keywords as well as access by key (keyword), the downside is that the key must be derived from the value),
      //  hence the value is not a simple string, but a KwItem instead. Alternatively, an OrderedDictionary could be considered, however .NET comes with only
      //  non-generic version of it and OrderedDictionary<TKey, TValue> class would have to be created "by hand".
      // TODO: Consider replacing KeyedCollection by a thread-safe collection (note that SynchronizedKeyedCollection is not available in .NET Core)

      /// <summary>
      /// Derives key from KwItem (required by KeyedCollection).
      /// </summary>
      /// <param name="item"></param>
      /// <returns></returns>
      protected override string GetKeyForItem(IItem item)
      {
         return item.Key;
      }

      /// <summary>
      /// Collection of contained items.
      /// </summary>
      new internal IList<IItem> Items
      {
         //Provide access to a protected member of base class.
         get { return base.Items; }

      }

      /// <summary>
      /// Collection of contained keys.
      /// </summary>
      internal IList<string> Keys
      {
         get { return base.Items.Select(i => GetKeyForItem(i)).ToList(); }
      }

      /// <summary>
      /// Replaces the item at the specified index with the specified item.
      /// </summary>
      /// <param name="index">Zero-based index of the item to be replaced.</param>
      /// <param name="item">New item.</param>
      new internal void SetItem(int index, IItem item)
      {
         base.SetItem(index, item);
      }

   }
}
