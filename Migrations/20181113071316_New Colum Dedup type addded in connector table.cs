using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class NewColumDeduptypeadddedinconnectortable : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "dedup_type",
                schema: "dedup",
                table: "Connectors",
                nullable: false,
                defaultValue: 0);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "dedup_type",
                schema: "dedup",
                table: "Connectors");
        }
    }
}
