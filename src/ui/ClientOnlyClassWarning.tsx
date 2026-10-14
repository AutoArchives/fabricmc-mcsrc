import { theme, Tooltip } from "antd";
import { useObservable } from "../utils/UseObservable";
import { clientOnlyClass } from "../logic/ServerData";

export const ClientOnlyClassWarning = () => {
    const { token } = theme.useToken();
    const isClientOnly = useObservable(clientOnlyClass);

    if (!isClientOnly) return null;

    return (
        <Tooltip title="This class is absent from the dedicated server jar.">
            <div style={{
                marginLeft: "16px",
                padding: "3px 8px",
                backgroundColor: token.colorInfoBg,
                color: token.colorInfoText,
                borderRadius: "4px",
                fontSize: "11px",
                fontWeight: 500,
                border: `1px solid ${token.colorInfoBorder}`,
                whiteSpace: "nowrap",
                flexShrink: 0,
                cursor: "help"
            }}>
                Client-only class
            </div>
        </Tooltip>
    );
};
