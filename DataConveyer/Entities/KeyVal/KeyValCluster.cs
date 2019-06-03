//KeyValCluster.cs
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
using System.Linq;

namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// A cluster consisting of a series of records.
   /// </summary>
   internal class KeyValCluster : ICluster
   {
      //Underlying collection of KeyValRecord objects:
      private readonly RecordCollection _recordColl;

      internal readonly TypeDefinitions _typeDefinitions;  // internal to allow ReadOnlyCluster ctor use the same type definitions
      internal readonly OrchestratorConfig _config;  // internal to allow ReadOnlyCluster ctor use the same config
      internal readonly Func<Phase, int, PhaseStatus> _processingStatusSupplier;   // internal to allow ReadOnlyCluster ctor (& record specific transform providers) use the same processingStatusSupplier

      //Note that KeyValCluster is constructed eagerly (recList will be consumed upon object creation)

      internal KeyValCluster(IEnumerable<IRecord> recList,
                             int clstrNo,
                             int startRecNo,
                             int startSourceNo,
                             IGlobalCache globalCache,
                             IDictionary<string, object> propertyBin,
                             TypeDefinitions typeDefinitions,
                             OrchestratorConfig config,
                             Func<Phase, int, PhaseStatus> processingStatusSupplier)
      {
         this.ClstrNo = clstrNo;
         this.StartRecNo = startRecNo;
         this.StartSourceNo = startSourceNo;
         //Note that StartSourceNo should match the 1st record, but to facilitate cloning, etc. it was decided to expose it
         // separately as ctor parm as opposed to reading it like this:  this.StartSourceNo = recList.Any() ? recList.First().SourceNo : 0;
         this._recordColl = new RecordCollection(recList);
         this.GlobalCache = globalCache;
         this._typeDefinitions = typeDefinitions;
         this._config = config;
         this._processingStatusSupplier = processingStatusSupplier;

         //Make sure all records have the ClstrNo matching the cluster they belong to:
         this._recordColl.ForEach(r => (r as KeyValRecord)?.SetClstrNo(clstrNo));

         PropertyBin = (_config.PropertyBinEntities & PropertyBinAttachedTo.Clusters) == PropertyBinAttachedTo.Clusters
               ? propertyBin ?? new Dictionary<string, object>()  //"reuse" PB in case of cloning, creation of ReadOnlyCluster wrapper, etc.
               : null;  //null if Clusters flag not set in PropertyBinEntities

      }  //ctor


#region ICluster implementation

      /// <summary>
      /// Cluster sequence number (1 based).
      /// </summary>
      public int ClstrNo { get; private set; }     //cluster seq# (1 based)

      /// <summary>
      /// Sequence number of the intake record that the cluster started at (1 based).
      /// </summary>
      public int StartRecNo { get; private set; }     //Seq# of the intake record that the cluster started at (1 based).

      /// <summary>
      /// Index number of the intake source that supplied the intake record that the cluster started at (1 based).
      /// </summary>
      public int StartSourceNo { get; private set; }

      /// <summary>
      /// Number of records contained in the cluster.
      /// </summary>
      public int Count
      {
         get { return _recordColl.Count; }
      }

      /// <summary>
      /// A set of key value pairs that are common to all records and clusters throughout the process execution.
      /// </summary>
      public IGlobalCache GlobalCache { get; private set; }

      /// <summary>
      /// The property bin associated with the cluster.
      /// </summary>
      public IDictionary<string, object> PropertyBin { get; private set; }

      /// <summary>
      /// A collection of records contained in the cluster.
      /// </summary>
      public IReadOnlyList<IRecord> Records
      {
         get { return _recordColl.AsReadOnly(); }
      }

      /// <summary>
      /// A record at a specified index position (0 based)
      /// </summary>
      /// <param name="index">Position of the record to get or set (0 based)</param>
      /// <returns>Record requested</returns>
      public IRecord this[int index]
      {
         get { return _recordColl[index]; }

         set { _recordColl[index] = value; }
      }

      /// <summary>
      /// Remove record at a specified index position from the cluster.
      /// </summary>
      /// <param name="index">Position of the record to remove (0 based).</param>
      /// <returns>True if removed, false if not (index out of range).</returns>
      public bool RemoveRecord(int index)
      {
         if (!InRange(index)) return false;
         _recordColl.RemoveAt(index);
         return true;
      }

      /// <summary>
      /// Add a given record at the end of the cluster.
      /// </summary>
      /// <param name="record">Record to add, which must originate from the cluster or its ancestor.</param>
      /// <returns>true if record successfully added, false if not (record rejected).</returns>
      public bool AddRecord(IRecord record)
      {
         //Note: ancestor is the cluster's parent in cloning. E.g.  IF clstr = ancestor.GetClone() THEN record can belong to either clstr or ancestor (or ancestor's ancestor, etc.)

         //TODO: Verify record if valid, i.e. came from the same cluster or its ancestor; if not, return false
         //      This is to prevent keys missing in FieldsInUse (as they are only maintained once items are added to records)
         _recordColl.Add(record);
         return true;
      }

      /// <summary>
      /// Return a record at a specified index position.
      /// Calling this method is equivalent to accessing indexer or the Records property except that in case index is out of range, it returns null instead of throwing OutOfRangeException.
      /// </summary>
      /// <param name="index">Index position of the record to return</param>
      /// <returns></returns>
      public IRecord GetRecord(int index)
      {
         if (!InRange(index)) return null;
         return _recordColl[index];
      }

      /// <summary>
      /// Return a copy (deep clone) of the cluster.
      /// </summary>
      /// <returns></returns>
      public ICluster GetClone()
      {
         return new KeyValCluster(_recordColl.Select(rec => ((KeyValRecord)rec).GetClone_body()), ClstrNo, StartRecNo, StartSourceNo, GlobalCache, PropertyBin, _typeDefinitions, _config, _processingStatusSupplier);
      }

      /// <summary>
      /// Return an empty cluster, i.e. cluster with no records, but the same characteristics (e.g. ClstrNo) as this cluster.
      /// </summary>
      /// <returns></returns>
      public ICluster GetEmptyClone()
      {
         return new KeyValCluster(Enumerable.Empty<KeyValRecord>(), ClstrNo, StartRecNo, StartSourceNo, GlobalCache, PropertyBin, _typeDefinitions, _config, _processingStatusSupplier);
      }

      /// <summary>
      /// Return an empty record (template) that is suitable to be contained in the current cluster.
      /// This method should only be used in case of empty cluster; otherwise, (a clone of) one of the contained records should be used instead. 
      /// </summary>
      /// <returns>An empty record that matches characteristics of current cluster.</returns>
      public IRecord ObtainEmptyRecord()
      {
         return new KeyValRecord(Enumerable.Empty<IItem>(), StartRecNo, StartSourceNo, ClstrNo, GlobalCache, null, null, _typeDefinitions, _config, _processingStatusSupplier, ActionOnDuplicateKey.IgnoreItem);
      }

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

#endregion ICluster implementation


      #region Private/internal methods

      /// <summary>
      /// Helper function to verify that index position points to existing record.
      /// </summary>
      /// <param name="index"></param>
      /// <returns>True if valid; false if not</returns>
      private bool InRange(int index)
      {
         return index >= 0 && index < _recordColl.Count;
      }

   }

#endregion Private/internal methods

}