using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class Deduped_count_added : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "deduped_count",
                schema: "dedup",
                table: "Connectors",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "deduped_count",
                schema: "dedup",
                table: "Connectors");
        }
    }
}
