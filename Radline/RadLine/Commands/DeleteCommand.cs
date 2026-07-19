namespace RadLine
{
    public sealed class DeleteCommand : LineEditorCommand
    {
        public override void Execute(LineEditorContext context)
        {
            var buffer = context.Buffer;

            // At the end of a line, delete pulls the following line up onto this one.
            // The editor decides whether that's possible (multiline, not the last line).
            if (buffer.AtEnd)
            {
                context.Submit(SubmitAction.Delete);
                return;
            }

            buffer.Clear(buffer.Position, 1);
        }
    }
}
