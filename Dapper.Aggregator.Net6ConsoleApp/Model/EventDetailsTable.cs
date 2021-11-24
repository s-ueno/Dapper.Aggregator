using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper.Aggregator.Net6ConsoleApp.Model
{
    [Index("idx_event_detail_table", new[] { nameof(EventID), nameof(DetailNo) })]
    [Table("event_detail_table")]
    public class EventDetailsTable
    {
        [Column(Name = "event_id", DDLType = "int", IsPrimaryKey = true)]
        public int EventID { get; set; }

        [Column(Name = "detail_no", DDLType = "int", IsPrimaryKey = true)]
        public int DetailNo { get; set; }

        [Column(Name = "cd", DDLType = "char(10)")]
        public string CD { get; set; }

        [Column(Name = "lockversion", DDLType = "int")]
        public int Lockversion { get; set; }
    }
}
