using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class DeDup_Added_new_Cloumn_Non_Deduped_Count : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "non_deduped_count",
                schema: "dedup",
                table: "Connectors",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "non_deduped_count",
                schema: "dedup",
                table: "Connectors");
        }
    }
}
