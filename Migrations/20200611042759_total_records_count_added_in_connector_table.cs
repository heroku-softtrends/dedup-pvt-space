using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class total_records_count_added_in_connector_table : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "total_records_count",
                schema: "dedup",
                table: "Connectors",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "total_records_count",
                schema: "dedup",
                table: "Connectors");
        }
    }
}
