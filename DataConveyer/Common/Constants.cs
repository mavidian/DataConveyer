//Constants.cs
//
// Copyright © 2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.using System;
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


using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;

namespace Mavidian.DataConveyer.Common
{
   /// <summary>
   /// Provides constant values that apply to Data Conveyer processing.
   /// </summary>
   public static class Constants
   {
      /// <summary>
      /// Value indicating no limit in <see cref="OrchestratorConfig">configuration</see> parameters,
      /// such as <see cref="OrchestratorConfig.BufferSize" /> or <see cref="OrchestratorConfig.IntakeRecordLimit" />.
      /// </summary>
      public const int Unlimited = -1;

      /// <summary>
      /// <see cref="ICluster.StartRecNo"/> value for the head cluster.
      /// </summary>
      public const int HeadClusterRecNo = 0;

      /// <summary>
      /// <see cref="ICluster.StartRecNo"/> value for the foot cluster.
      /// </summary>
      public const int FootClusterRecNo = -1;
   }
}
