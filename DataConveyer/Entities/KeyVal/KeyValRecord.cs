//KeyValRecord.cs
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
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;

namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// Represents a single record consisting of KeyVal items, e.g. line of a keyword data
   /// </summary>
   internal class KeyValRecord : DynamicObject, IRecord, IUntypedRecord
   {
      //Underlying collection of KeyValItem objects:
      private readonly ItemCollection _itemColl;

      private readonly TypeDefinitions _typeDefinitions;
      private readonly OrchestratorConfig _config;
      private readonly Func<Phase, int, PhaseStatus> _processingStatusSupplier;

      private readonly bool _fieldsCanBeAltered;  //Reflects config.AllowTransformToAlterFields.
                                                  // If true, fields can be added to and removed from the record;
                                                  // if false, AddItem (flavors) and RemoveItem methods have no effect.

      /// <summary>
      /// Constructs a single record consisting of key value pairs.
      /// </summary>
      /// <param name="items">A collection of KeyValItems (a set of key value pairs) obtained from a single input record.</param>
      /// <param name="recNo">Record number, i.e. position in input data (1-based).</param>
      /// <param name="sourceNo">Index number of the source that supplied this record (1-based).</param> 
      /// <param name="clstrNo">Sequential number (1-based) that the record is initially assigned (e.g. XML or JSON input); 0=undetermined;.</param>
      /// <param name="globalCache">A set of key value pairs that are common to all records and clusters throughout the process execution.</param> 
      /// <param name="traceBin">The trace bin to be used by this record.</param> 
      /// <param name="propertyBin">The property bin to be used by this record.</param> 
      /// <param name="typeDefinitions">Field type definitions (name -> type translations).</param> 
      /// <param name="config">Current orchestrator configuration.</param>
      /// <param name="processingStatusSupplier">Function to return the PhaseStatus object.</param>
      /// <param name="actionOnDuplicateKey">Action to take in case duplicate key is encountered; same as config.ActionOnDuplicateKey except for cloning.</param>
      internal KeyValRecord(IEnumerable<IItem> items,
                            int recNo,
                            int sourceNo,
                            int clstrNo,
                            IGlobalCache globalCache,
                            IReadOnlyDictionary<string, object> traceBin,
                            IDictionary<string, object> propertyBin,
                            TypeDefinitions typeDefinitions,
                            OrchestratorConfig config,
                            Func<Phase, int, PhaseStatus> processingStatusSupplier,
                            ActionOnDuplicateKey actionOnDuplicateKey)
      {
         _itemColl = new ItemCollection();
         GlobalCache = globalCache;
         _typeDefinitions = typeDefinitions;
         _config = config;
         _processingStatusSupplier = processingStatusSupplier;
         _fieldsCanBeAltered = config.AllowTransformToAlterFields;
         RecNo = recNo;
         SourceNo = sourceNo;
         TargetNo = 0;  //not yet determined
         ClstrNo = clstrNo;
         int fldNo = 0;
         foreach (IItem item in items)
         {
            fldNo++;
            var key = item.Key;
            if (_itemColl.Contains(key))  //not thread-safe, but we're in ctor so that nothing else can access _itemColl
            {
               //Duplicate key encountered, the action is dictated by actionOnDuplicateKey
               string msgOnDemand() => $"Duplicate key in field #{fldNo} of record #{recNo}: A key of '{item.Key.ToString()}' already exists in current record.";
               switch (actionOnDuplicateKey)
               {
                  case ActionOnDuplicateKey.IgnoreItem:
                     //Do nothing, except for reporting the error
                     _config.Logger.LogWarning(() => msgOnDemand() + $" Item containing value of '{item.Value.ToString()}' has been ignored.");
                     break;
                  case ActionOnDuplicateKey.ReplaceItem:
                     //remove existing item and then add the new item
                     _config.Logger.LogWarning(() => msgOnDemand() + $" Item has been replaced with a value of '{item.Value.ToString()}'.");
                     _itemColl.Remove(item.Key);
                     _itemColl.Add(item);
                     break;
                  case ActionOnDuplicateKey.ExcludeRecord:
                     //Clear the items constructed so far and exit; the caller will ignore such record
                     _itemColl.Clear();
                     _config.Logger.LogError(() => msgOnDemand() + "Record has been excluded from processing.");
                     return;
                  case ActionOnDuplicateKey.AssignDefaultKey:
                     throw new NotImplementedException("Feature to provide substitutes for duplicate keys has not been implemented.");
                     //TODO: Implement this feature - note that any "default" value can also already exist
               };
            }
            else  //The key is not a duplicate, so simply add the item
            {
               _itemColl.Add(item);
            }
         }  //foreach item

         TraceBin = traceBin;  //"reuse" TB in case of cloning, creation of ReadOnlyRecord wrapper, etc.
         PropertyBin = (_config.PropertyBinEntities & PropertyBinAttachedTo.Records) == PropertyBinAttachedTo.Records
                        ? propertyBin ?? new Dictionary<string, object>()  //"reuse" PB in case of cloning, creation of ReadOnlyRecord wrapper, etc.
                        : null;  //null if Records flag not set in PropertyBinEntities

      }  //ctor


      /// <summary>
      /// Assign cluster number to the record.
      /// Intended to only be called from the cluster constructor (e.g. while in the clustering block) to sync the actual sequence numbers.
      /// </summary>
      /// <param name="clstrNo"></param>
      internal void SetClstrNo(int clstrNo)
      {
         ClstrNo = clstrNo;
      }


#region IRecordBase implementation

      /// <summary>
      /// Sequence number of the record on intake (1 based); retained upon cloning during transformation
      /// </summary>
      public int RecNo { get; private set; }  //accurate as long as intake process is single threaded

      /// <summary>
      /// Index number of the intake source that supplied the record (1 based).
      /// </summary>
      public int SourceNo { get; private set; }

      /// <summary>
      /// Index number of the output target that the record is sent to (1 based).
      /// </summary>
      public int TargetNo { get; internal set; }

      /// <summary>
      /// Sequential cluster number (1-based) that may be assigned at Intake, such as in case of XML or JSON data.
      /// For other input data kinds it is initially 0, but then it is assigned the actual cluster number by the clustering block (at end of Intake phase).
      /// </summary>
      public int ClstrNo { get; private set;  }

      /// <summary>
      /// A collection of keys contained in the record.
      /// </summary>
      public IReadOnlyList<string> Keys
      {
         get
         {
            return _itemColl.Keys.ToList().AsReadOnly();
         }
      }

      /// <summary>
      /// A collection of items contained in the record.
      /// </summary>
      public IReadOnlyList<IItem> Items
      {
         get
         {
            if (_itemColl.Count == 0) return new List<IItem>().AsReadOnly();  //TODO: Not thread-safe, verify and fix if needed
            return _itemColl.Items.ToList().AsReadOnly();
         }
      }

      /// <summary>
      /// Number of items contained in the record
      /// </summary>
      public int Count
      {
         get { return _itemColl.Count; }
      }

      /// <summary>
      /// A set of key value pairs that are common to all records and clusters throughout the process execution.
      /// </summary>
      public IGlobalCache GlobalCache { get; private set; }

      /// <summary>
      /// The property bin associated with the record.
      /// </summary>
      public IDictionary<string, object> PropertyBin { get; private set; }


      /// <summary>
      /// Return true if the record contains an item with the specified key; otherwise, return false.
      /// </summary>
      /// <param name="key"></param>
      /// <returns></returns>
      public bool ContainsKey(string key)
      {
         return _itemColl.Contains(key);
      }


      /// <summary>
      /// Remove item for a specified key
      /// </summary>
      /// <param name="key">Key of the item to remove</param>
      /// <returns>true if the element is successfully removed, false if item not found, null if item additions/removals are disallowed</returns>
      public bool? RemoveItem(string key)
      {
         if (!this._fieldsCanBeAltered) return null;
         return _itemColl.Remove(key);
      }


      /// <summary>
      /// Replace an existing item with a new one
      /// </summary>
      /// <param name="item">New item</param>
      /// <returns>True if item replaced, otherwise (key not found in record or void item) return false</returns>
      public bool ReplaceItem(IItem item)
      {
         //note the absence of key or index parameter; this is because item already contains the key, so that it would've been redundant
         if (item is VoidKeyValItem) return false;  //TODO: Verify if this is correct action (alternative might be to remove item or even to ignore this special case altogether)
         int index = IndexForKey(item.Key);
         if (index == -1) return false;
         _itemColl.SetItem(index, item);
         return true;
      }

      /// <summary>
      /// Include the item in the record by either adding it or replacing existing item with the same key
      /// </summary>
      /// <param name="item">Item to add or replace</param>
      /// <returns>True if an item with the same key existed (item has been replaced); otherwise return false if item has been added or null if item has not been added because additions/removals are disallowed.</returns>
      public bool? AddOrReplaceItem(IItem item)
      {
         int index = IndexForKey(item.Key);
         if (index == -1)
         {  //key does not exist
            if (!_fieldsCanBeAltered) return null;
            _itemColl.Add(item);
            return false;
         }
         _itemColl.SetItem(index, item);
         return true;
      }


      /// <summary>
      /// Return an item for a specified key
      /// </summary>
      /// <param name="key">A key of the item to return</param>
      /// <returns>An item or a void item if no such item exist</returns>
      public IItem GetItem(string key)
      {
         return _itemColl.Contains(key) ? _itemColl[key] : new VoidKeyValItem(key);
      }


      /// <summary>
      /// Return an item at a specified index position
      /// </summary>
      /// <param name="index">Index position of the item to return (0-based)</param>
      /// <returns> An item or a void item if index is out of range</returns>
      public IItem GetItem(int index)
      {
         return InRange(index) ? _itemColl[index] : new VoidKeyValItem(null);
      }

      /// <summary>
      /// Trace bin, i.e. a dictionary intended to contain key value pairs collected during processing of preceding records on intake.
      /// This property remains null unless set within the <see cref="Mavidian.DataConveyer.Orchestrators.OrchestratorConfig.RecordInitiator"/> function.
      /// </summary>
      public IReadOnlyDictionary<string, object> TraceBin { get; private set; }

      /// <summary>
      /// Returns processing status of a given phase.
      /// </summary>
      /// <param name="phase">One of: Intake, Transformation or Output.</param>
      /// <param name="transformerNo">Transformer number (0-based); optional, if omitted, then 0, i.e. first transformer assumed ; irrelevant in case of Intake or Output.</param>
      /// <returns>Immutable PhaseStatus object that describes the current status of processing or a given phase.</returns>
      public PhaseStatus GetProcessingStatus(Phase phase, int transformerNo = 0)
      {
         return _processingStatusSupplier(phase, transformerNo);
      }

#endregion IRecordBase implementation


#region IRecord implementation

      /// <summary>
      /// A value of an item for a specified key.
      /// Attempt to set a value of a non-existing item  (i.e. when item for a key does not exist) has no effect
      /// </summary>
      /// <param name="key">A key of the item</param>
      /// <returns>A typed value or null if item for a key does not exist</returns>
      public object this[string key]
      {
         get { return _itemColl.Contains(key) ? _itemColl[key].Value : null; }
         set { if (_itemColl.Contains(key)) SetItemValue(IndexForKey(key), value); }
      }


      /// <summary>
      /// A value of an item at a specified index position.
      /// Attempt to set a value of a non-existing item (i.e. when index is out of range) has no effect.
      /// </summary>
      /// <param name="index">An index position of the item to get or set (0-based)</param>
      /// <returns>A typed value or null if index is out of range</returns>
      public object this[int index]
      {
         get { return InRange(index) ? _itemColl[index].Value : null; }
         set { if (InRange(index)) SetItemValue(index, value); }
      }


      /// <summary>
      /// Add an item for a given key and value at the end of the record
      /// </summary>
      /// <param name="key">Key of an item to add</param>
      /// <param name="value">Typed value of an item to add</param>
      /// <returns>The item added or void item if key already existed (and thus no item was added); if item was not added because additions/removals are disallowed return null.</returns>
      public IItem AddItem(string key, object value)
      {
         return AddItem_body(key, () => KeyValItem.CreateItem(key, value, _typeDefinitions));
      }


      /// <summary>
      /// Return a clone of a given item with a new value.
      /// </summary>
      /// <param name="item">Item to clone</param>
      /// <param name="value">New (typed) value for the clone</param>
      /// <returns>A clone of a given item.</returns>
      public IItem GetItemClone(IItem item, object value)
      {
         //cloning should be confined to items "owned" by record
         if(!_itemColl.Contains(item))
         {
            //this can happen if client app uses an item from a different record of a cluster
            _config.Logger.LogWarning(() => $"Attempt to clone item '{item.Key}' within record #{RecNo}, but there is no such item in the record.");
            return new VoidKeyValItem(item.Key);
         }
         return KeyValItem.CreateItem(item.Key, value, _typeDefinitions);
      }


      /// <summary>
      /// Return a copy (deep clone) of current record.
      /// </summary>
      /// <returns>A deep clone of the current record</returns>
      public IRecord GetClone()
      {
         return GetClone_body();
      }


      /// <summary>
      /// Return an empty clone of current record. Caution is advised in case field additions/removals are disallowed (AllowTransformToAlterFields=false, which is a default value), as the record returned by this method is unmaintainable, i.e. it will remain empty forever.
      /// </summary>
      /// <returns>A record with no items, but the same RecNo as the current record.</returns>
      public IRecord GetEmptyClone()
      {
         return GetEmptyClone_body();
      }


      /// <summary>
      /// Return an empty X12 segment for a given name and number of elements.
      /// </summary>
      /// <param name="name">Segment type (name), e.g. NM1.</param>
      /// <param name="elementCount">Number of elements in the segment (each element is assigned an empty string value).</param>
      /// <returns>An empty record (with the same RecNo as the current record) representing X12 segment.</returns>
      public IRecord CreateEmptyX12Segment(string name, int elementCount)
      {
         return CreateEmptyX12Segment_body(name, elementCount);
      }

      /// <summary>
      /// Return an X12 segment containing a given text.
      /// </summary>
      /// <param name="contents">The contents of the entire segment with DefaultX12FieldDelimiter between fields, but no segment delimiter at end.</param>
      /// <param name="fieldDelimiter">Field delimiter character used within contents.</param>
      /// <returns>A record (with the same RecNo as on current record) representing X12 segment.</returns>
      public IRecord CreateFilledX12Segment(string contents, char fieldDelimiter = '\0')
      {
         return CreateFilledX12Segment_body(contents, fieldDelimiter);
      }

#endregion IRecord implementation


#region IUntypedRecord implementation

      /// <summary>
      /// A value of an item for a specified key.
      /// Attempt to set a value of a non-existing item  (i.e. when item for a key does not exist) has no effect
      /// </summary>
      /// <param name="key">A key of the item</param>
      /// <returns>A string representation of the value or null if item for a key does not exist</returns>
      string IUntypedRecord.this[string key]
      {
         get { return _itemColl.Contains(key) ? _itemColl[key].StringValue : null; }
         set { if (_itemColl.Contains(key)) SetItemValue(IndexForKey(key), value); }
      }


      /// <summary>
      /// A value of an item at a specified index position.
      /// Attempt to set a value of a non-existing item (i.e. when index is out of range) has no effect.
      /// </summary>
      /// <param name="index">An index position of the item to get or set (0-based)</param>
      /// <returns>A string representation of the value or null if index is out of range</returns>
      string IUntypedRecord.this[int index]
      {
         get { return InRange(index) ? _itemColl[index].StringValue : null; }
         set { if (InRange(index)) SetItemValue(index, value); }
      }

      /// <summary>
      /// Add an item for a given key and value at the end of the record
      /// </summary>
      /// <param name="key">Key of an item to add</param>
      /// <param name="value">String representation of a value of an item to add</param>
      /// <returns>The item added or void item if key already existed (and thus no item was added); if item was not added because additions/removals are disallowed return null.</returns>
      IItem IUntypedRecord.AddItem(string key, string value)
      {
         return AddItem_body(key, () => KeyValItem.CreateItem(key, value, _typeDefinitions));
      }

      /// <summary>
      /// Return a clone of a given item with a new value
      /// </summary>
      /// <param name="item">Item to clone</param>
      /// <param name="value">String representation of a new value for the clone</param>
      /// <returns>Cloned item</returns>
      IItem IUntypedRecord.GetItemClone(IItem item, string value)
      {
         return KeyValItem.CreateItem(item.Key, value, _typeDefinitions);
      }


      /// <summary>
      /// Return a copy (deep clone) of the record
      /// </summary>
      /// <returns>A deep clone of current record</returns>
      IUntypedRecord IUntypedRecord.GetClone()
      {
         return GetClone_body();
      }


      /// <summary>
      /// Return an empty clone of current record
      /// </summary>
      /// <returns>A record with no items, but the same RecNo as current record</returns>
      IUntypedRecord IUntypedRecord.GetEmptyClone()
      {
         return GetEmptyClone_body();
      }


      /// <summary>
      /// Return an empty X12 segment
      /// </summary>
      /// <param name="name">Segment type (name), e.g. NM1.</param>
      /// <param name="elementCount">Number of elements in the segment.</param>
      /// <returns>An empty record representing X12 segment.</returns>
      IUntypedRecord IUntypedRecord.CreateEmptyX12Segment(string name, int elementCount)
      {
         return CreateEmptyX12Segment_body(name, elementCount);
      }

      /// <summary>
      /// Return an X12 segment containing a given text.
      /// </summary>
      /// <param name="contents">The contents of the entire segment with DefaultX12FieldDelimiter between fields, but no segment delimiter at end.</param>
      /// <param name="fieldDelimiter">Field delimiter character used within contents.</param>
      /// <returns>A record (with the same RecNo as on current record) representing X12 segment.</returns>
      IUntypedRecord IUntypedRecord.CreateFilledX12Segment(string contents, char fieldDelimiter)
      {
         return CreateFilledX12Segment_body(contents, fieldDelimiter);
      }

#endregion IUntypedRecord implementation


#region Implementation of dynamic properties (DynamicObject)

      /// <summary>
      /// Return a value of a dynamic property, i.e. an item for a specified key (or null if no such item exist).
      /// Implementation of DynamicObject.TryGetMember.
      /// </summary>
      /// <param name="binder"></param>
      /// <param name="result"></param>
      /// <returns></returns>
      public override bool TryGetMember(GetMemberBinder binder, out object result)
      {
         var key = binder.Name;
         if (_itemColl.Contains(key))
         {
            result = _itemColl[key].Value;
            return true;
         }
         result = null;
         return true;    //TODO: Consider returning false here to indicate the property (field) does not exist (research how this should be handled)
      }


      /// <summary>
      /// Assign a value for a dynamic property, i.e. an item for a specified key.
      /// </summary>
      /// <param name="binder"></param>
      /// <param name="value"></param>
      /// <returns></returns>
      public override bool TrySetMember(SetMemberBinder binder, object value)
      {
         var key = binder.Name;
         //TODO: Not thread-safe, verify and fix if needed
         if (_itemColl.Contains(key))
         {
            SetItemValue(IndexForKey(key), value);
         }
         else  //add a property "on-the-fly", but only if allowed (config.AllowTransformToAlterFields is true)
         {
            if (!this._fieldsCanBeAltered) return true;  //TODO: Consider returning false to indicate adding field was disallowed (research how this should be handled)
            _itemColl.Add(KeyValItem.CreateItem(key, value, _typeDefinitions));
         }
         return true;
      }

#endregion Implementation of dynamic properties


#region Private/internal methods

      /// <summary>
      /// Helper method to replace existing item with a new one
      /// </summary>
      /// <param name="index"></param>
      /// <param name="newItem"></param>
      private void SetItemValue(int index, IItem newItem)
      {
         Debug.Assert(_itemColl[index].Key == newItem.Key);
         _itemColl.SetItem(index, newItem);
      }
      /// <summary>
      /// Helper method to replace existing item with a new one created from a submitted string value
      /// </summary>
      /// <param name="index">Position of the item to replace (0 based)</param>
      /// <param name="newValue"></param>
      private void SetItemValue(int index, string newValue)
      {
         SetItemValue(index, KeyValItem.CreateItem(KeyForIndex(index), newValue, _typeDefinitions));
      }
      /// <summary>
      /// Helper method to replace existing item with a new one created from a submitted strongly typed value
      /// </summary>
      /// <param name="index">Position of the item to replace (0 based)</param>
      /// <param name="newValue"></param>
      private void SetItemValue(int index, object newValue)
      {
         SetItemValue(index, KeyValItem.CreateItem(KeyForIndex(index), newValue, _typeDefinitions));
      }

      /// <summary>
      /// Return position of a Key (field name) in the record
      /// </summary>
      /// <param name="key">Key (field name) of an item in this record</param>
      /// <returns>0 based index position or -1 if key not found</returns>
      private int IndexForKey(string key)
      {
         return ContainsKey(key) ? _itemColl.IndexOf(_itemColl[key]) : -1;
      }

      /// <summary>
      /// Helper method to return key (field name) at a given position in the record
      /// </summary>
      /// <param name="index">Position of an item in this record(0 based)</param>
      /// <returns>A key at a given index or null if index out of range</returns>
      private string KeyForIndex(int index)
      {
         return InRange(index) ?_itemColl.Keys[index] : null;
      }

      // The ".._body" methods below contain "bodies" (common parts) of the .. methods defined in both IRecord and IUntypedRecord interfaces
      // The difference between .. methods in these interfaces is in parameter/retval types (IRecord - object, IUntypedRecord - string)


      /// <summary>
      /// Body of AddItem methods
      /// </summary>
      /// <param name="key"></param>
      /// <param name="itemCreator">Function to create item</param>
      /// <returns>Item added or null if item was not added because additions/removals are disallowed.</returns>
      private IItem AddItem_body(string key, Func<IItem> itemCreator)
      {
         if (!this._fieldsCanBeAltered) return null;
         if (ContainsKey(key)) return new VoidKeyValItem(key); //TODO: not thread-safe, verify and fix if needed
         var item = itemCreator();
         _itemColl.Add(item);
         return item;
      }


      /// <summary>
      ///  Body of GetClone methods (also called by KeyValCluster.GetClone method).
      /// </summary>
      /// <returns></returns>
      internal KeyValRecord GetClone_body()
      {
         return new KeyValRecord(Items.Select(itm => KeyValItem.CreateItem(itm.Key, itm.Value, _typeDefinitions)), RecNo, SourceNo, ClstrNo, GlobalCache, TraceBin, PropertyBin, _typeDefinitions, _config, _processingStatusSupplier, ActionOnDuplicateKey.IgnoreItem);
         //Note that ActionOnDuplicateKey.IgnoreItem is appropriate as no dups are possible in existing KeyValRecord (IOW, we don't need something like _actionOnDuplicateKey)
      }


      /// <summary>
      /// Body of GetEmptyClone methods.
      /// </summary>
      /// <returns></returns>
      private KeyValRecord GetEmptyClone_body()
      {
         //In case fieldsCanBeAltered is false, GetEmptyClone creates unmaintainable empty record (not a good thing typically, issue a warning)
         if (!_fieldsCanBeAltered)
         {
            _config.Logger.LogWarning(() => $"Unmaintainable empty record created by GetEmpyClone method for record #{RecNo} (consider setting AllowTransformToAlterFields=true)" );
         }
         return new KeyValRecord(Enumerable.Empty<IItem>(), RecNo, SourceNo, ClstrNo, GlobalCache, TraceBin, PropertyBin, _typeDefinitions, _config, _processingStatusSupplier, ActionOnDuplicateKey.IgnoreItem);
         //note that ActionOnDuplicateKey.IgnoreItem is appropriate as no dups are possible in empty list (IOW, we don't need something like _actionOnDuplicateKey)
      }


      /// <summary>
      /// Body of CreateX12Segment methods that create empty X12 segment
      /// </summary>
      /// <param name="name"></param>
      /// <param name="elementCount"></param>
      /// <returns></returns>
      private KeyValRecord CreateEmptyX12Segment_body(string name, int elementCount)
      {
         var elems = Enumerable.Repeat(KeyValItem.CreateItem("Segment", name, _typeDefinitions), 1).
                     Concat(Enumerable.Range(0, elementCount).Select(i => KeyValItem.CreateItem(string.Format("Elem{0:000}", i+1), string.Empty, _typeDefinitions)));
         //alternatively: var elems = Enumerable.Range(0, elementCount + 1).Select(i => KeyValItem.CreateItem(i == 0 ? "Segment" : string.Format("Elem{0:000}", i), i == 0 ? name : string.Empty, _typeDefinitions));
         //Note that each element has empty string assigned (however, user can assign null afterwards).
         return new KeyValRecord(elems, RecNo, SourceNo, ClstrNo, GlobalCache, null, null, this._typeDefinitions, this._config, _processingStatusSupplier, ActionOnDuplicateKey.IgnoreItem);
         //note that ActionOnDuplicateKey.IgnoreItem is appropriate as no dups are possible in a list of consecutively named keys, i.e. Segment, Elem001, Elem002, ...
      }

      /// <summary>
      /// Body of CreateX12Segment methods that take entire segment contents as a parameter
      /// </summary>
      /// <param name="contents">The contents of the entire segment using DefaultX12FieldDelimiter between fields, but no segment delimiter at end.</param>
      /// <param name="fieldDelimiter">Field delimiter character used within contents.</param>
      /// <returns></returns>
      private KeyValRecord CreateFilledX12Segment_body(string contents, char fieldDelimiter)
      {
         //note that null char is the default value for _config.DefaultX12FieldDelimiter
         var delimiterInEffect = fieldDelimiter == '\0' ? _config.DefaultX12FieldDelimiter == '\0' ? '*' : _config.DefaultX12FieldDelimiter : fieldDelimiter;
         var i = 0;
         var elems = contents.Split(delimiterInEffect).Select(v => KeyValItem.CreateItem(i++ == 0 ? "Segment" : string.Format("Elem{0:000}", i-1), v, _typeDefinitions));
         return new KeyValRecord(elems, RecNo, SourceNo, ClstrNo, GlobalCache, null, null, _typeDefinitions, _config, _processingStatusSupplier, ActionOnDuplicateKey.IgnoreItem);
         //note that ActionOnDuplicateKey.IgnoreItem is appropriate as no dups are possible in a list of consecutively named keys, i.e. Segment, Elem001, Elem002, ...
      }


      /// <summary>
      /// Helper function to verify that index position is valid for an item
      /// </summary>
      /// <param name="index"></param>
      /// <returns>True if valid; false if not</returns>
      private bool InRange(int index)
      {
         return index >= 0 && index < _itemColl.Count;
      }

      /// <summary>
      /// Save provided set of values in the record's trace bin
      /// </summary>
      /// <param name="traceBinTemplate">Dictionary containing key value pairs to be saved in the trace bin (a clone of this dictionary will be created)</param>
      internal void SetTraceBin(IDictionary<string,object> traceBinTemplate)
      {
         TraceBin = new ReadOnlyDictionary<string, object>(new Dictionary<string,object>(traceBinTemplate));
      }

#endregion Private/internal methods

   }
}
