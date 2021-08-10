using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class non_deduped_count_renamed_to_sync_updated_count : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameColumn(
                name: "non_deduped_count",
                schema: "dedup",
                table: "Connectors",
                newName: "sync_updated_count");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameColumn(
                name: "sync_updated_count",
                schema: "dedup",
                table: "Connectors",
                newName: "non_deduped_count");
        }
    }
}
