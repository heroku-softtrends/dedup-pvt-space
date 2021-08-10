using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class dedup_process_count : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "dedup_process_count",
                schema: "dedup",
                table: "Connectors",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "dedup_process_count",
                schema: "dedup",
                table: "Connectors");
        }
    }
}
