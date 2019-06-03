//ReadOnlyRecord.cs
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


using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Dynamic;

namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// Immutable wrapper over KeyValRecord
   /// </summary>
   internal class ReadOnlyRecord : DynamicObject, IRecord, IUntypedRecord
   {
      private readonly KeyValRecord _realRecord;
      private readonly bool _throwOnMisuse;

      internal ReadOnlyRecord(KeyValRecord realRecord, bool throwOnMisuse)
      {
         _realRecord = realRecord;
         _throwOnMisuse = throwOnMisuse;
      }  //ctor

      internal ReadOnlyRecord(KeyValRecord realRecord) : this(realRecord, false) { }  //creates instance that ignores errors

#region IRecordBase implementation

      public int RecNo { get { return _realRecord.RecNo; } }

      public int SourceNo { get { return _realRecord.SourceNo; } }

      public int TargetNo { get { return _realRecord.TargetNo; } }

      public int ClstrNo { get { return _realRecord.ClstrNo; } }

      public IReadOnlyList<string> Keys { get { return _realRecord.Keys; } }

      public IReadOnlyList<IItem> Items { get { return _realRecord.Items; } }

      public int Count { get { return _realRecord.Count; } }

      public IGlobalCache GlobalCache {  get { return _realRecord.GlobalCache; } }  //note that elements of ReadOnlyRecord.GlobalCache are writable

      public IDictionary<string, object> PropertyBin { get { return _realRecord.PropertyBin; } }  //note that elements of ReadOnlyRecord.PropertyBin are writable

      public bool ContainsKey(string key) { return _realRecord.ContainsKey(key); }

      public bool? RemoveItem(string key) { return ThrowOrIgnore<bool?>(null); }

      public bool ReplaceItem(IItem item) { return ThrowOrIgnore(false); }

      public bool? AddOrReplaceItem(IItem item) { return ThrowOrIgnore<bool?>(null); }

      public IItem GetItem(string key) { return _realRecord.GetItem(key); }

      public IItem GetItem(int index) { return _realRecord.GetItem(index); }

      public IReadOnlyDictionary<string, object> TraceBin { get { return _realRecord.TraceBin; } }

      public PhaseStatus GetProcessingStatus(Phase phase, int transformerNo = 0) { return _realRecord.GetProcessingStatus(phase, transformerNo); }

#endregion IRecordBase implementation


#region IRecord implementation

      public object this[string key]
      {
         get { return _realRecord[key]; }
         set { ThrowOrIgnore(); }
      }

      public object this[int index]
      {
         get { return _realRecord[index]; }
         set { ThrowOrIgnore(); }
      }

      public IItem AddItem(string key, object value) { return ThrowOrIgnore<IItem>(null); }

      public IItem GetItemClone(IItem item, object value) { return _realRecord.GetItemClone(item, value); } 

      public IRecord GetClone() { return ThrowOrIgnore<IRecord>(null); }

      public IRecord GetEmptyClone() { return ThrowOrIgnore<IRecord>(null); }

      public IRecord CreateEmptyX12Segment(string name, int elementCount) { return ThrowOrIgnore<IRecord>(null); }

      public IRecord CreateFilledX12Segment(string contents, char fieldDelimiter) { return ThrowOrIgnore<IRecord>(null); }

#endregion IRecord implementation


#region IUntypedRecord implementation

      string IUntypedRecord.this[string key]
      {
         get { return ((IUntypedRecord)_realRecord)[key]; }
         set { ThrowOrIgnore(); }
      }


      string IUntypedRecord.this[int index]
      {
         get { return ((IUntypedRecord)_realRecord)[index]; }
         set { ThrowOrIgnore(); }
      }

      IItem IUntypedRecord.AddItem(string key, string value) { return ThrowOrIgnore<IItem>(null); }

      IItem IUntypedRecord.GetItemClone(IItem item, string value) { return _realRecord.GetItemClone(item, value); } 

      IUntypedRecord IUntypedRecord.GetClone() { return ThrowOrIgnore<IUntypedRecord>(null); }

      IUntypedRecord IUntypedRecord.GetEmptyClone() { return ThrowOrIgnore<IUntypedRecord>(null); }

      IUntypedRecord IUntypedRecord.CreateEmptyX12Segment(string name, int elementCount) { return ThrowOrIgnore<IUntypedRecord>(null); }

      IUntypedRecord IUntypedRecord.CreateFilledX12Segment(string contents, char fieldDelimiter) { return ThrowOrIgnore<IUntypedRecord>(null); }

#endregion IUntypedRecord implementation


#region Implementation of dynamic properties

      public override bool TryGetMember(GetMemberBinder binder, out object result)
      {
         bool retVal = _realRecord.TryGetMember(binder, out object realResult);
         result = realResult;
         return retVal;
      }

      public override bool TrySetMember(SetMemberBinder binder, object value)
      {
         return ThrowOrIgnore(true);
      }

#endregion Implementation of dynamic properties


      private T ThrowOrIgnore<T>(T value)
      {
         ThrowOrIgnore();
         return value;
      }

      private void ThrowOrIgnore()
      {
         if (_throwOnMisuse) throw new NotSupportedException("Unsupported operation invoked on ReadOnlyRecord object.");
      }

   }
}
