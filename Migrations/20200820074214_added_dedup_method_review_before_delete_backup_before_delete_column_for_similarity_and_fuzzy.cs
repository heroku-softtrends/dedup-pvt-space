using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class added_dedup_method_review_before_delete_backup_before_delete_column_for_similarity_and_fuzzy : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "backup_before_delete",
                schema: "dedup",
                table: "Connectors",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<int>(
                name: "dedup_method",
                schema: "dedup",
                table: "Connectors",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<int>(
                name: "review_before_delete",
                schema: "dedup",
                table: "Connectors",
                nullable: false,
                defaultValue: 0);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "backup_before_delete",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "dedup_method",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "review_before_delete",
                schema: "dedup",
                table: "Connectors");
        }
    }
}
