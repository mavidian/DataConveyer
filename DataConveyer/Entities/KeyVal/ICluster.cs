//ICluster.cs
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
using System.Collections.Generic;

namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// Interface defining a record cluster
   /// </summary>
   public interface ICluster
   {
      /// <summary>
      /// Sequence number of the cluster on intake (1 based).  This number remains unchanged throughout the processing; for example in case of cluster cloning, the clone will inherit ClstrNo from the original cluster.
      /// </summary>
      int ClstrNo { get; }
      /// <summary>
      /// Sequence number of the first record in cluster on intake (1 based).
      /// This number remains unchanged throughout the processing; for example in case of cluster cloning, the clone will inherit StartRecNo from the original cluster.
      /// There are 2 special values: 0 (<see cref="Constants.HeadClusterRecNo"/>) means head cluster and -1 (<see cref="Constants.FootClusterRecNo"/>) means foot cluster.
      /// </summary>
      int StartRecNo { get; }
      /// <summary>
      /// Index number (1 based) of the intake source that supplied the first record of the cluster.
      /// This number remains unchanged throughout the processing; for example in case of record cloning, the clone will inherit SourceNo from the original cluster.
      /// </summary>
      int StartSourceNo { get; }
      /// <summary>
      /// Number of records contained in the cluster.
      /// </summary>
      int Count { get;  }
      /// <summary>
      /// A set of key value pairs that are common to all records and clusters throughout the process execution.
      /// Elements of global cache are defined via the <see cref="OrchestratorConfig.GlobalCacheElements"/> setting.
      /// Global cache also allows signals to synchronize thread of Data Conveyer processing.
      /// </summary>
      IGlobalCache GlobalCache { get; }
      /// <summary>
      /// The property bin associated with the cluster.
      /// Property bin is a dictionary containing arbitrary set of key value pairs that can be added and removed throughout of DataConveyer processing.
      /// In order to have clusters contain property bin objects, the <see cref="OrchestratorConfig.PropertyBinEntities"/> setting must contain the Clusters flag;
      /// otherwise the property bin will be null.
      /// </summary>
      IDictionary<string, object> PropertyBin { get; }
      /// <summary>
      /// A collection of records contained in the cluster.
      /// </summary>
      IReadOnlyList<IRecord> Records { get; }
      /// <summary>
      /// A record at a specified index position (0-based).
      /// </summary>
      /// <param name="index">Position of the record (0-based).</param>
      /// <returns></returns>
      IRecord this[int index] { get; set; }
      /// <summary>
      /// Remove record at a specified index position from the cluster.
      /// </summary>
      /// <param name="index">Position of the record to remove (0-based)</param>
      /// <returns>true if removed, false if not (index out of range).</returns>
      bool RemoveRecord(int index);
      /// <summary>
      /// Add a given record at the end of the cluster.
      /// </summary>
      /// <param name="record">Record to add, which must originate from the cluster or its ancestor.</param>
      /// <returns>true if record successfully added, false if not (record rejected).</returns>
      bool AddRecord(IRecord record);
      /// <summary>
      /// Return a record at a specified index position.
      /// </summary>
      /// <param name="index">Position of record to get (0-based).</param>
      /// <returns>Record at a given index or null if index out of range.</returns>
      IRecord GetRecord(int index);
      /// <summary>
      /// Return a copy (deep clone) of the cluster.
      /// The cloned cluster will have the same characteristics (e.g. ClstrNo and PropertyBin) as the current cluster.
      /// </summary>
      /// <returns>An cluster containing (copies of) the same records as in the current cluster.</returns>
      ICluster GetClone();
      /// <summary>
      /// Return an empty cluster, i.e. cluster with no records, but the same characteristics (e.g. ClstrNo and PropertyBin) as the current cluster.
      /// </summary>
      /// <returns>An empty cluster (with no records).</returns>
      ICluster GetEmptyClone();
      /// <summary>
      /// Return an empty record (template) that is suitable to be contained in the current cluster.
      /// <note type="caution">
      /// This method should only be used in case of an empty cluster, such as a head cluster.
      /// Otherwise, one of the records contained in the cluster should be cloned instead. 
      /// </note>
      /// </summary>
      /// <returns>An empty record that matches characteristics of current cluster.</returns>
      IRecord ObtainEmptyRecord();
      /// <summary>
      /// Return processing status of a given phase.
      /// </summary>
      /// <param name="phase">One of: Intake, Transformation or Output.</param>
      /// <param name="transformerNo">Transformer number (0-based); optional, if omitted, then 0, i.e. first transformer assumed ; irrelevant in case of Intake or Output.</param>
      /// <returns>Immutable PhaseStatus object that describes the current status of processing or a given phase.</returns>
      PhaseStatus GetProcessingStatus(Phase phase, int transformerNo = 0);
   }
}
