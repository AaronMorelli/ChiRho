/*
   Copyright 2016 Aaron Morelli

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [CoreXR].[ProfilerTraceEvents](
	[EventGroup] [nvarchar](40) NOT NULL,
	[trace_event_id] [smallint] NOT NULL,
	[event_name] [nvarchar](128) NOT NULL,
	[category_name] [nvarchar](128) NOT NULL,
	[isEnabled] [nchar](1) NOT NULL,
 CONSTRAINT [PKProfilerTraceEvents] PRIMARY KEY CLUSTERED 
(
	[EventGroup] ASC,
	[trace_event_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING ON
GO
CREATE UNIQUE NONCLUSTERED INDEX [AKProfileTraceEvents] ON [CoreXR].[ProfilerTraceEvents]
(
	[EventGroup] ASC,
	[event_name] ASC,
	[category_name] ASC
)
INCLUDE ( 	[isEnabled]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [CoreXR].[ProfilerTraceEvents]  WITH CHECK ADD  CONSTRAINT [CK_ProfilerTraceEvents_isEnabled] CHECK  (([isEnabled]=N'N' OR [isEnabled]=N'Y'))
GO
ALTER TABLE [CoreXR].[ProfilerTraceEvents] CHECK CONSTRAINT [CK_ProfilerTraceEvents_isEnabled]
GO
