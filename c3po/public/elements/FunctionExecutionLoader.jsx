import { Loader2 } from "lucide-react";

export default function FunctionExecutionLoader() {
  // If not executing, don't render anything
  if (!props.isExecuting) return null;

  return (
    <div className="flex items-center gap-3 p-4 rounded-md border border-border bg-muted/50">
      <div className="relative">
        <Loader2 className="h-5 w-5 animate-spin text-primary" />
      </div>
      <div className="flex flex-col">
        <span className="font-medium">{props.functionName || "Function"}</span>
        <span className="text-sm text-muted-foreground">{props.message || "Executing function..."}</span>
      </div>
    </div>
  );
}