using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper.Aggregator.Net6ConsoleApp.Model
{
    [Index("idx_event_table", new[] { nameof(ID), nameof(Title) })]
    [Table("event_table")]
    public class EventTable
    {
        [Column(Name = "id", DDLType = "int", IsPrimaryKey = true)]
        public int ID { get; set; }

        [Column(Name = "title", DDLType = "varchar(255)")]
        public string Title { get; set; }

        [Column(Name = "event_time", DDLType = "timestamp with time zone")]
        public DateTime EventTime { get; set; }

        [Column(Name = "lockversion", DDLType = "int")]
        public int Lockversion { get; set; }
    }
}
