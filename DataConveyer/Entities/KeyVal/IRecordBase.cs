//IRecordBase.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using System.Collections.Generic;

namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// Base interface for IRecord and IUntypedRecord interfaces
   /// </summary>
   public interface IRecordBase
   {
      /// <summary>
      /// Sequence number of the record on intake (1 based).  This number remains unchanged throughout the processing; for example in case of record cloning, the clone will inherit RecNo from the original record.
      /// </summary>
      int RecNo { get; }
      /// <summary>
      /// Index number of the intake source that supplied the record (1 based).  This number remains unchanged throughout the processing; for example in case of record cloning, the clone will inherit SourceNo from the original record.
      /// </summary>
      int SourceNo { get; }
      /// <summary>
      /// Index number of the output target that the record is sent to (1 based). Before output phase (ClusterRouter function), the value is 0, which means not yet determined.
      /// </summary>
      int TargetNo { get; }
      /// <summary>
      /// 1-based sequential cluster number (on intake it is 0 (undetermined), unless assigned in case of XML intake); the actual number is assigned at clustering block.
      /// </summary>
      int ClstrNo { get; }
      /// <summary>
      /// A collection of keys contained in the record.
      /// </summary>
      IReadOnlyList<string> Keys { get; }
      /// <summary>
      ///  A collection of items contained in the record.
      /// </summary>
      IReadOnlyList<IItem> Items { get; }
      /// <summary>
      /// Number of items contained in the record
      /// </summary>
      int Count { get; }
      /// <summary>
      /// A set of key value pairs that are common to all records and clusters throughout the process execution.
      /// Elements of global cache are defined via the <see cref="OrchestratorConfig.GlobalCacheElements"/> seting.
      /// Global cache also allows signals to synchronize thread of Data Conveyer processing.
      /// </summary>
      IGlobalCache GlobalCache { get; }
      /// <summary>
      /// The property bin associated with the record.
      /// Property bin is a dictionary containing arbitrary set of key value pairs that can be added and removed throughout of DataConveyer processing.
      /// In order to have records contain property bin objects, the <see cref="OrchestratorConfig.PropertyBinEntities"/> setting must contain the Records flag;
      /// otherwise the property bin will be null.
      /// </summary>
      IDictionary<string, object> PropertyBin { get; }
      /// <summary>
      /// Verify if the record contains an item with a given key.
      /// </summary>
      /// <param name="key">Key to verify</param>
      /// <returns>True if the record contains an item with the specified key; otherwise false.</returns>
      bool ContainsKey(string key);
      /// <summary>
      /// Remove item for a specified key.
      /// </summary>
      /// <param name="key">Key of an item to remove</param>
      /// <returns>True if the item was successfully removed; false item not found, null if item additions/removals are disallowed.</returns>
      bool? RemoveItem(string key);
      /// <summary>
      /// Replace an existing item with a new one.
      /// </summary>
      /// <param name="item">A new item (to replace the existing item for the same key).</param>
      /// <returns>True if the item was successfully replaced; otherwise (key not found in record) false.</returns>
      bool ReplaceItem(IItem item);
      /// <summary>
      /// Include the item in the record by either adding it or replacing existing item with the same key.
      /// </summary>
      /// <param name="item">A new item (to replace the existing item for the same key).</param>
      /// <returns>True if an item with the same key existed before (item has been replaced); otherwise return false if item has been added or null if item has not been added because additions/removals are disallowed.</returns>
      bool? AddOrReplaceItem(IItem item);
      /// <summary>
      /// Obtain an item for a specified key.
      /// </summary>
      /// <param name="key">Key of an item to obtain.</param>
      /// <returns>The item for a specified key or a void item if no such item exist.</returns>
      IItem GetItem(string key);
      /// <summary>
      /// Obtain an item at a specified index position.
      /// </summary>
      /// <param name="index">Index position of the item to obtain (0-based).</param>
      /// <returns>The item at a specified index position or a void item if index is out of range.</returns>
      IItem GetItem(int index);
      /// <summary>
      /// The trace bin object associated with the record. It can be set up in the <see cref="OrchestratorConfig.RecordInitiator"/> function.
      /// Trace bin is a dictionary intended to contain key value pairs collected during processing of preceding records on intake.
      /// Note that for efficiency empty trace bin objects are not attached; in such cases, TraceBin values are null.
      /// </summary>
      IReadOnlyDictionary<string, object> TraceBin { get; }
      /// <summary>
      /// Return processing status of a given phase.
      /// </summary>
      /// <param name="phase">One of: Intake, Transformation or Output.</param>
      /// <param name="transformerNo">Transformer number (0-based); optional, if omitted, then 0, i.e. first transformer assumed ; irrelevant in case of Intake or Output.</param>
      /// <returns>Immutable PhaseStatus object that describes the current status of processing or a given phase.</returns>
      PhaseStatus GetProcessingStatus(Phase phase, int transformerNo = 0);
   }
}
